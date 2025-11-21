import time
from collections import defaultdict
import avro.schema
import avro.io
import io
from confluent_kafka import Consumer, Producer
from flask import json

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

class AvroConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'avro-consumer-group',
            'auto.offset.reset': 'earliest'
        })
        
        self.dlq_producer = Producer({
            'bootstrap.servers': 'localhost:29092'
        })
        
        self.consumer.subscribe(['orders'])
        
        self.price_totals = defaultdict(float)
        self.order_counts = defaultdict(int)

        self.retry_count = {}
        self.max_retries = 3
        
        self.stats = {'processed': 0, 'failed': 0, 'dlq': 0}
        
    def avro_deserialize(self, bytes_data):
        """Deserialize Avro binary data to Python dict"""
        bytes_reader = io.BytesIO(bytes_data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)
    
    def avro_serialize(self, data):
        """Serialize data to Avro binary format"""
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()
        
    def calculate_running_average(self, product, price):
        self.price_totals[product] += price
        self.order_counts[product] += 1
        average = self.price_totals[product] / self.order_counts[product]
        
        print(f"📊 {product}: Average updated to ${average:.2f} (from {self.order_counts[product]} orders)")
        return average
        
    def display_statistics(self):
        print("\n" + "=" * 70)
        print("📈 REAL-TIME ORDER ANALYTICS - AVRO")
        print("=" * 70)
        
        if not self.price_totals:
            print("  No orders processed yet")
        else:
            for product in sorted(self.price_totals.keys()):
                avg = self.price_totals[product] / self.order_counts[product]
                total = self.price_totals[product]
                count = self.order_counts[product]
                print(f"  {product:<12} Average: ${avg:>8.2f} (Total: ${total:>8.2f}, Orders: {count:>3})")
        
        print("-" * 70)
        print(f"  Processed: {self.stats['processed']} | Failed: {self.stats['failed']} | DLQ: {self.stats['dlq']}")
        print("=" + "=" * 69 + "\n")
    
    def process_order(self, order):
        """Process order with failure simulation"""
        price = order['price']
        
        if price > 600:
            raise Exception("Simulated temporary failure: price too high")
        
        return True
    
    def handle_message(self, message_value):
        try:
            # Deserialize Avro binary data
            order = self.avro_deserialize(message_value)
            order_id = order['orderId']
            product = order['product']
            price = order['price']
            
            print(f"📨 Avro Order: {order_id} - {product} - ${price:.2f}")
            
            max_retries = 3
            attempt = 0
            processed = False
            last_error = None
            
            while attempt < max_retries and not processed:
                attempt += 1
                try:
                    print(f"🔄 Attempt {attempt}/{max_retries} for {order_id}")
                    self.process_order(order)
                    processed = True
                    print(f"✅ Success: {order_id}")
                    
                    # REAL-TIME AGGREGATION
                    self.calculate_running_average(product, price)
                    self.stats['processed'] += 1
                    
                except Exception as e:
                    last_error = e
                    print(f"❌ Attempt {attempt} failed: {e}")
                    if attempt < max_retries:
                        time.sleep(2)  # Wait before retry
            
            if not processed:
                dlq_message = {
                    "orderId": order_id,
                    "product": product,
                    "price": price,
                    "error": str(last_error),
                    "retry_attempts": attempt,
                    "timestamp": time.time()
                }
                dlq_data = json.dumps(dlq_message).encode('utf-8')
                self.dlq_producer.produce('orders-dlq', value=dlq_data)
                self.dlq_producer.poll(0)
                self.stats['dlq'] += 1
                print(f"💀 Sent to DLQ: {order_id}")
            
            return processed
            
        except Exception as e:
            print(f"💥 Error processing Avro message: {e}")
            self.stats['failed'] += 1
            return False
    
    def run(self):
        print("👂 AVRO CONSUMER STARTED")
        print("=" * 60)
        print("Kafka: localhost:29092")
        print("Topic: orders")
        print("Serialization: AVRO BINARY")
        print("Features:")
        print("✅ Real Avro serialization/deserialization")
        print("✅ Real-time price averages per product")
        print("✅ Retry logic (3 attempts)")
        print("✅ Dead Letter Queue (orders-dlq topic)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    print(f"⚠️ Kafka error: {msg.error()}")
                    continue
                
                self.handle_message(msg.value())
                
                # Show stats every 5 processed messages
                if self.stats['processed'] > 0 and self.stats['processed'] % 5 == 0:
                    self.display_statistics()
                    
        except KeyboardInterrupt:
            print("\n🛑 Avro Consumer stopped")
            self.display_statistics()
        except Exception as e:
            print(f"💥 Fatal error: {e}")
        finally:
            self.consumer.close()
            self.dlq_producer.flush()

if __name__ == "__main__":
    consumer = AvroConsumer()
    consumer.run()