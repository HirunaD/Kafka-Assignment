import random
import time
import avro.schema
import avro.io
import io
from confluent_kafka import Producer

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

class AvroProducer:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': 'localhost:29092',
            'message.timeout.ms': 5000
        })
        self.products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Tablet", "Printer"]
        self.order_count = 0
        
    def avro_serialize(self, data):
        """Serialize data to Avro binary format"""
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()
        
    def delivery_report(self, err, msg):
        if err:
            print(f'❌ Delivery failed: {err}')
        else:
            print(f'✅ Delivered to {msg.topic()} [{msg.partition()}]')
    
    def create_order(self):
        self.order_count += 1
        return {
            "orderId": f"AVRO-{self.order_count:06d}",
            "product": random.choice(self.products),
            "price": round(random.uniform(50.0, 800.0), 2)
        }
    
    def send_order(self):
        try:
            order = self.create_order()
            # Serialize to Avro binary
            avro_data = self.avro_serialize(order)
            
            self.producer.produce(
                topic='orders',
                value=avro_data,  
                callback=self.delivery_report
            )
            
            self.producer.poll(0)
            print(f"📦 Avro Order: {order}")
            return True
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def run(self):
        print("🚀 AVRO PRODUCER STARTED")
        print("=" * 60)
        print("Kafka: localhost:29092")
        print("Topic: orders")
        print("Serialization: AVRO BINARY")
        print("Producing 1 order every 3 seconds...")
        print("Press Ctrl+C to stop\n")
        
        stats = {'sent': 0, 'errors': 0}
        
        try:
            while True:
                if self.send_order():
                    stats['sent'] += 1
                else:
                    stats['errors'] += 1
                
                total = stats['sent'] + stats['errors']
                if total % 5 == 0:
                    print(f"📊 Stats: {stats['sent']} sent, {stats['errors']} errors")
                
                time.sleep(3)
                
        except KeyboardInterrupt:
            print(f"\n🛑 Producer stopped. Final: {stats['sent']} sent, {stats['errors']} errors")
        finally:
            self.producer.flush()

if __name__ == "__main__":
    producer = AvroProducer()
    producer.run()