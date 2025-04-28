from kafka import KafkaConsumer
import json


# 初始化 Consumer
consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'],
    auto_offset_reset='earliest',  # 從最舊的訊息開始讀
    enable_auto_commit=True,
    group_id='order-consumer-group',
    value_deserializer=lambda x: x.decode('utf-8') if x else None
)

# 開始消費消息
for message in consumer:
    if message.value:
        print(f"Raw message: {message.value}")
        try:
            order = json.loads(message.value)  # 解析為 JSON
            print(
                f"Order ID: {order['order_id']}, "
                f"Customer: {order['customer_name']}, "
                f"Product: {order['product_name']}, "
                f"Quantity: {order['quantity']}, "
                f"Price: {order['price']}, "
                f"Time: {order['timestamp']}"
            )
        except json.JSONDecodeError:
            print("Received an invalid or empty JSON message.")
            continue
    else:
        print("Received an empty message.")
