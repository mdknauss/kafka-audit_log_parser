from confluent_kafka import Consumer, KafkaError
import json
import sys

# Configuration for Confluent Cloud Kafka consumer
conf = {
    'bootstrap.servers': 'your_bootstrap_servers:9092',  # Replace with your Confluent Cloud bootstrap servers
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'your_api_key',  # Replace with your Confluent Cloud API key
    'sasl.password': 'your_api_secret',  # Replace with your Confluent Cloud API secret
    'group.id': 'your_consumer_group',  # Replace with a consumer group ID
    'auto.offset.reset': 'earliest'  # Start from the beginning of the topic
}

# The specific string to search for within the principal
principal_search_string = 'your_search_string_here'  # Replace with the string to search for in the principal

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the audit log topic
consumer.subscribe(['audit-log'])  # Assuming the topic name is 'audit-log'; adjust if different

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% Reached end of partition\n')
            else:
                # Error
                print(f"Error: {msg.error()}")
            continue

        # Decode the message value (assuming UTF-8 encoded JSON)
        message_value = msg.value().decode('utf-8')
        
        try:
            # Parse the JSON message
            audit_log = json.loads(message_value)
            
            # Check for the principal in the data.authenticationInfo field
            principal = audit_log.get('data', {}).get('authenticationInfo', {}).get('principal', None)
            
            # Check if principal exists and is a string before performing the comparison
            if principal and isinstance(principal, str) and principal_search_string.lower() in principal.lower():
                print(f"Matching message for principal containing '{principal_search_string}':")
                print(json.dumps(audit_log, indent=4))
                print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}\n")
            else:
                if principal and not isinstance(principal, str):
                    print(f"Non-string principal detected: {principal}")
                else:
                    print(f"Non-matching principal: '{principal}'")
                print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}\n")
        except json.JSONDecodeError:
            print(f"Failed to parse JSON: {message_value}")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()