import json
from confluent_kafka import Consumer
import sys

class DLQConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'dlq-monitor',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['orders-dlq'])
        
    def display_dlq_messages(self):
        """Display all messages in DLQ"""
        print("💀 DEAD LETTER QUEUE MONITOR")
        print("=" * 70)
        print("Listening for failed messages...")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    print(f"⚠️ Kafka error: {msg.error()}")
                    continue
                
                try:
                    dlq_message = json.loads(msg.value().decode('utf-8'))
                    
                    print("❌ PERMANENTLY FAILED ORDER:")
                    print(f"   Order ID: {dlq_message['orderId']}")
                    print(f"   Product:  {dlq_message['product']}")
                    print(f"   Price:    ${dlq_message['price']:.2f}")
                    print(f"   Error:    {dlq_message.get('error', 'Unknown error')}")
                    print(f"   Retries:  {dlq_message.get('retry_attempts', 0)} attempts")
                    print("   " + "=" * 50)
                    
                except Exception as e:
                    print(f"💥 Could not parse DLQ message: {e}")
                    
        except KeyboardInterrupt:
            print("\n🛑 DLQ Monitor stopped")
        finally:
            self.consumer.close()

    def show_dlq_summary(self):
        """Show current DLQ status and exit"""
        print("💀 DEAD LETTER QUEUE - CURRENT STATUS")
        print("=" * 70)
        
        dlq_messages = []
        start_time = time.time()
        
        # Collect recent DLQ messages
        while (time.time() - start_time) < 5:  # Listen for 5 seconds
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            try:
                dlq_message = json.loads(msg.value().decode('utf-8'))
                dlq_messages.append(dlq_message)
            except:
                pass
        
        if not dlq_messages:
            print("   No failed messages in DLQ")
        else:
            print(f"   Total failed messages: {len(dlq_messages)}")
            print("\n   Recent failed orders:")
            for i, msg in enumerate(dlq_messages[-10:], 1):  # Show last 10
                print(f"   {i}. {msg['orderId']} - {msg['product']} - ${msg['price']:.2f}")
                print(f"      Error: {msg.get('error', 'Unknown')}")
        
        print("=" * 70)
        self.consumer.close()

if __name__ == "__main__":
    import time
    
    dlq_consumer = DLQConsumer()
    
    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        dlq_consumer.show_dlq_summary()
    else:
        dlq_consumer.display_dlq_messages()