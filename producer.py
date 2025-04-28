from kafka import KafkaProducer
import json
import time
import random
import uuid


# 假資料
customers = ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
products = ['iPhone', 'iPad', 'MacBook', 'Apple Watch', 'AirPods']


def generate_order():
    order = {
        "order_id": str(uuid.uuid4()),                      
        "customer_name": random.choice(customers),          
        "product_name": random.choice(products),            
        "quantity": random.randint(1, 5),                   
        "price": round(random.uniform(100, 2000), 2),       
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')    
    }

    return order


# 初始化 Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9091'],  # kafka1
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 持續發送訊息
for _ in range(10):
    order_data = generate_order()
    try:
        producer.send('orders-topic', order_data)
        print(f"Sent order: {order_data}")
    except Exception as e:
        print(f"Failed to send message: {e}")
    time.sleep(1)

# 關閉 Producer
producer.flush()
producer.close()
