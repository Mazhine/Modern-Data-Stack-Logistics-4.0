"""
Logistics 4.0 Data Streaming Manager.
Responsible for orchestrating the injection of logistics data into the Kafka cluster.
Supports nominal data streams, stochastic anomaly injection, and model inference simulation.
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
from kafka import KafkaProducer
import logging_loki


# ==============================================================================
# CONFIGURATION
# ==============================================================================
LOKI_URL = "http://localhost:3100/loki/api/v1/push"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'dataco_orders'
DATASET_FILE_PATH = '../DataCoSupplyChainDataset.csv'
CURSOR_FILE = 'stream_cursor.txt'

# Configure Loki Emitter
logging_loki.emitter.LokiEmitter.level_tag = "level"
loki_handler = logging_loki.LokiHandler(
    url=LOKI_URL,
    tags={"application": "logistics_pipeline", "component": "streaming", "env": "production"},
    version="1",
)
logger = logging.getLogger("stream_manager")
logger.setLevel(logging.DEBUG)
logger.addHandler(loki_handler)
# Console handler for local visibility
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class DataCorruptor:
    """Handles stochastic data anomaly injection for pipeline resilience testing."""
    
    @staticmethod
    def inject_anomaly(msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Injects specific business anomalies into the payload based on a uniform distribution.
        
        Args:
            msg (Dict[str, Any]): The original valid payload.
            
        Returns:
            Dict[str, Any]: The corrupted payload.
        """
        anomaly_chance: int = random.randint(1, 100)
        
        if anomaly_chance <= 10:
            msg['Order Item Total'] = -500.0
            logger.error(f"[ANOMALY] Negative Order Item Total injected for Order ID {msg.get('Order Id')}")
        elif anomaly_chance <= 20:
            msg['Delivery Status'] = "Lost in Space"
            logger.error(f"[ANOMALY] Invalid Delivery Status injected for Order ID {msg.get('Order Id')}")
        elif anomaly_chance <= 30:
            msg['order date (DateOrders)'] = "3026-03-01 10:00:00"
            logger.error(f"[ANOMALY] Future Order Date timestamp injected for Order ID {msg.get('Order Id')}")
        elif anomaly_chance <= 40:
            original_id = msg['Order Id']
            msg['Order Id'] = None
            logger.error(f"[ANOMALY] NULL Order ID injected (Origin ID: {original_id})")
            
        return msg


class StreamingManager:
    """Manages the lifecycle and execution of the data stream."""
    
    def __init__(self, mode: str):
        self.mode = mode
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def _load_and_prepare_dataset(self) -> Optional[pd.DataFrame]:
        """Loads the historical dataset, processes dates, and shifts to current time projection."""
        if not os.path.exists(DATASET_FILE_PATH):
            logger.critical(f"Dataset path {DATASET_FILE_PATH} does not exist.")
            return None
            
        logger.info(f"Loading historic dataset from {DATASET_FILE_PATH}")
        df = pd.read_csv(DATASET_FILE_PATH, encoding='latin1')
        
        columns_to_keep = [
            'Order Id', 'Customer Id', 'Product Card Id',
            'order date (DateOrders)', 'shipping date (DateOrders)', 'Days for shipping (real)', 'Days for shipment (scheduled)',
            'Benefit per order', 'Sales per customer', 'Order Item Total', 'Order Item Discount', 'Product Price',
            'Order Status', 'Delivery Status', 'Shipping Mode', 'Late_delivery_risk',
            'Customer Country', 'Customer City', 'Order Country', 'Order City', 'Order Region',
            'Category Name', 'Customer Segment', 'Department Name', 'Type'
        ]
        df = df[columns_to_keep]
        
        # Enforce date types
        df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'])
        df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'])
        
        # Time-travel projection to emulate live ongoing streams
        max_date_in_data = df['order date (DateOrders)'].max()
        target_date = datetime(2026, 3, 1)
        time_offset = target_date - max_date_in_data
        
        df['order date (DateOrders)'] = df['order date (DateOrders)'] + time_offset
        df['shipping date (DateOrders)'] = df['shipping date (DateOrders)'] + time_offset
        df = df.sort_values(by='order date (DateOrders)')
        
        return df

    def execute_stream(self) -> None:
        """Starts the main delivery loop to Kafka Brokers."""
        df = self._load_and_prepare_dataset()
        if df is None:
            return
            
        logger.info(f"Initiating Live Stream in mode: [{self.mode.upper()}]")
        
        # Implement persistent pointer logic
        start_index = 0
        if os.path.exists(CURSOR_FILE):
            try:
                with open(CURSOR_FILE, 'r') as f:
                    start_index = int(f.read().strip())
                logger.info(f"Resuming streaming from index cursor: {start_index}")
            except Exception as e:
                logger.warning(f"Failed to read cursor file: {e}. Defaulting to 0.")
                start_index = 0
                
        df = df.iloc[start_index:]
        
        try:
            for idx_offset, (_, row) in enumerate(df.iterrows()):
                msg = row.to_dict()
                
                # Format dates for Kafka payload conformity
                msg['order date (DateOrders)'] = msg['order date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
                msg['shipping date (DateOrders)'] = msg['shipping date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
                
                # Apply anomalies based on executed configuration
                inject_anomaly = False
                if self.mode == 'chaos':
                    inject_anomaly = True
                elif self.mode == 'mix':
                    inject_anomaly = random.choice([True, False])
                    
                if inject_anomaly:
                    msg = DataCorruptor.inject_anomaly(msg)
                elif self.mode == 'ia':
                    logger.info(f"[INFERENCE] Dispatching payload {msg.get('Order Id')} to Spark MLlib.")
                else:
                    logger.info(f"[NOMINAL] Payload valid and dispatched. ID: {msg.get('Order Id')}")
                
                self.producer.send(KAFKA_TOPIC, msg)
                
                # Persistent state saving
                with open(CURSOR_FILE, 'w') as f:
                    f.write(str(start_index + idx_offset + 1))
                
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Stream gracefully terminated by operator.")
        except Exception as e:
            logger.error(f"Stream encountered a critical failure: {e}", exc_info=True)
        finally:
            self.producer.flush()
            self.producer.close()


def parse_arguments() -> argparse.Namespace:
    """Parses Command Line Arguments."""
    parser = argparse.ArgumentParser(description="Logistics Streaming Data Generative Tool")
    parser.add_argument(
        '--mode', 
        choices=['sain', 'chaos', 'mix', 'ia'], 
        required=True, 
        help="Stream execution modes: 'sain' (clean), 'chaos' (anomalies), 'mix', 'ia' (ML inference focus)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    manager = StreamingManager(mode=args.mode)
    manager.execute_stream()
