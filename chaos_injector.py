import pandas as pd
import json
import time
import os
import random
import logging
import logging_loki
from kafka import KafkaProducer
from datetime import datetime

# ==============================================================================
# CONFIGURATION OBSERVABILITÉ (GRAFANA LOKI)
# ==============================================================================
logging_loki.emitter.LokiEmitter.level_tag = "level"
loki_handler = logging_loki.LokiHandler(
    url="http://localhost:3100/loki/api/v1/push",
    tags={"application": "chaos_injector", "component": "streaming", "env": "production"},
    version="1",
)
logger = logging.getLogger("chaos-logger")
logger.setLevel(logging.ERROR)
logger.addHandler(loki_handler)

def simulate_chaos_stream():
    """
    Simulation Time-Travel des flux logistiques vers Kafka.
    VERSION CHAOS : Injecte volontairement des erreurs pour tester la rsilience de Spark et dbt.
    Toute erreur est transmise via l'API standard au système Loki.
    """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    file_path = '../DataCoSupplyChainDataset.csv'
    if not os.path.exists(file_path):
        print(f"ERREUR : {file_path} introuvable. Placez-le dans le dossier /PFA 2026/")
        return
        
    print("Chargement du dataset en cours...")
    df = pd.read_csv(file_path, encoding='latin1')
    
    cols = [
        'Order Id', 'Customer Id', 'Product Card Id',
        'order date (DateOrders)', 'shipping date (DateOrders)', 'Days for shipping (real)', 'Days for shipment (scheduled)',
        'Benefit per order', 'Sales per customer', 'Order Item Total', 'Order Item Discount', 'Product Price',
        'Order Status', 'Delivery Status', 'Shipping Mode', 'Late_delivery_risk',
        'Customer Country', 'Customer City', 'Order Country', 'Order City', 'Order Region',
        'Category Name', 'Customer Segment', 'Department Name', 'Type'
    ]
    df = df[cols]
    
    df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'])
    df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'])
    
    print("\n==============================================")
    print("Dmarrage du CHAOS Streaming Live vers Kafka + Loki")
    print("Attention: Les donnes vont tre sciemment corrompues")
    print("==============================================\n")
    
    for index, row in df.iterrows():
        msg = row.to_dict()
        msg['order date (DateOrders)'] = msg['order date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
        msg['shipping date (DateOrders)'] = msg['shipping date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
        
        # INJECTION DE CHAOS
        chaos_dice = random.randint(1, 100)
        
        if chaos_dice <= 10:
            msg['Order Item Total'] = -500.0
            print("=> CHAOS INJECT: Order Item Total = -500.0")
            logger.error("CHAOS INJECTION: Negative Order Item Total detected for Order ID " + str(msg.get('Order Id')))
            
        elif chaos_dice <= 20:
            msg['Delivery Status'] = "Lost in Space"
            print("=> CHAOS INJECT: Delivery Status = 'Lost in Space'")
            logger.error("CHAOS INJECTION: Unknown Delivery Status 'Lost in Space' for Order ID " + str(msg.get('Order Id')))
            
        elif chaos_dice <= 30:
            msg['order date (DateOrders)'] = "3026-03-01 10:00:00"
            print("=> CHAOS INJECT: Time travel -> 3026")
            logger.error("CHAOS INJECTION: Absurd Future Order Date for Order ID " + str(msg.get('Order Id')))
            
        elif chaos_dice <= 40:
            original_id = msg['Order Id']
            msg['Order Id'] = None
            print("=> CHAOS INJECT: Order Id = NULL")
            logger.error(f"CHAOS INJECTION: Missing Data - Order ID nullified (Originally {original_id})")
        
        producer.send('dataco_orders', msg)
        print(f"[*] Chaos Stream -> Order Id: {msg['Order Id']} | Status: {msg['Delivery Status']}")
        
        time.sleep(1)

if __name__ == "__main__":
    simulate_chaos_stream()
