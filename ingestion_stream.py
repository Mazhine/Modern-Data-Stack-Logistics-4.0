import pandas as pd
import json
import time
import os
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker

def simulate_logistics_stream():
    """
    Simulation Time-Travel des flux logistiques vers Kafka.
    Convertit le JSON DataCo 2018 vers Mars 2026.
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
    
    # Gouvernance de la Donne : Extraction des 25 Colonnes Strictes
    cols = [
        'Order Id', 'Customer Id', 'Product Card Id',
        'order date (DateOrders)', 'shipping date (DateOrders)', 'Days for shipping (real)', 'Days for shipment (scheduled)',
        'Benefit per order', 'Sales per customer', 'Order Item Total', 'Order Item Discount', 'Product Price',
        'Order Status', 'Delivery Status', 'Shipping Mode', 'Late_delivery_risk',
        'Customer Country', 'Customer City', 'Order Country', 'Order City', 'Order Region',
        'Category Name', 'Customer Segment', 'Department Name', 'Type'
    ]
    df = df[cols]
    
    # ----------------------------------------------------
    # Mcanique de TIME-TRAVEL (Projection vers Mars 2026)
    # ----------------------------------------------------
    df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'])
    df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'])
    
    max_date_in_data = df['order date (DateOrders)'].max()
    target_date = datetime(2026, 3, 1) # Objectif Mars 2026 demand
    time_offset = target_date - max_date_in_data
    
    df['order date (DateOrders)'] = df['order date (DateOrders)'] + time_offset
    df['shipping date (DateOrders)'] = df['shipping date (DateOrders)'] + time_offset
    
    df = df.sort_values(by='order date (DateOrders)') 
    
    print("\n==============================================")
    print("Dmarrage du Streaming Live (Mars 2026) vers Kafka")
    print("Topic Kafka Cible : 'dataco_orders'")
    print("==============================================\n")
    
    for index, row in df.iterrows():
        msg = row.to_dict()
        # Srialisation scurise des dates pour JSON
        msg['order date (DateOrders)'] = msg['order date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
        msg['shipping date (DateOrders)'] = msg['shipping date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
        
        producer.send('dataco_orders', msg)
        print(f"[*] Stream -> Order Id: {msg['Order Id']} | Date: {msg['order date (DateOrders)']} | Mode: {msg['Shipping Mode']}")
        
        # Variabilit dynamique du flux (Stochastique)
        time.sleep(0.5)

if __name__ == "__main__":
    simulate_logistics_stream()
