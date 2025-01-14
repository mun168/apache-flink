from faker import Faker
from confluent_kafka import SerializingProducer
import random
from datetime import datetime
import time
import json

fake = Faker()

def generate_sales_transactions():
    user  = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "producerId" : random.choice(['producer1', 'producer2', 'producer3' , 'producer4' , 'producer5' , 'producer6']),
        "productName" : random.choice(['lapto', 'smartphone', 'tablet', 'smartwatch', 'headphones', 'speaker']),
        'productCategory' : random.choice(['electronics', 'clothing', 'accessories', 'home', 'sports', 'beauty']),
        "price" : round(random.uniform(10,1000), 2),
        "productQuantity" : random.randint(1, 10),
        "productBrand" : random.choice(['apple', 'samsung', 'sony', 'lg', 'bose', 'jbl']),
        "currency" : random.choice(['USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD']),
        "customerId" : user['username'],
        "transactionDate":datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "paymentMethod" : random.choice(['credit_card', 'paypal', 'cash', 'bank_transfer']),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
        return
    print(f"Message produced: {msg.topic}: {msg.partition()}")

def main():
    topic = 'sales_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
    })

    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 120:
        try:
            transaction  = generate_sales_transactions()
            transaction['totalAmount'] = transaction['price'] * transaction['productQuantity']
            print(transaction)

            producer.produce( 
                topic=topic,
                key=transaction['transactionId'],
                value=json.dumps(transaction),
                on_delivery=delivery_report
            )
            producer.poll(0)

            #wait for 5 seconds before sending the next transaction

            time.sleep(5)
        except BufferError as e:
            print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")
            
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()