from confluent_kafka import Consumer, KafkaError
import json
import sys
import os

# Retrieve configuration from environment variables
conf = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS', 'your_bootstrap_servers:9092'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY', 'your_api_key'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET', 'your_api_secret'),
    'group.id': os.getenv('CONFLUENT_CONSUMER_GROUP', 'your_consumer_group'),
    'auto.offset.reset': 'earliest'  # Start from the beginning of the topic
}

# Sensitive resource prefix to filter for (e.g., topics with sensitive data)
sensitive_resource_prefix = os.getenv('SENSITIVE_RESOURCE_PREFIX', 'sensitive-')  # Replace or customize via env var

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the audit log topic
consumer.subscribe(['confluent-audit-log-events'])  # Standard audit log topic name; adjust if different

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
            
            # Extract key fields
            principal = audit_log.get('data', {}).get('authenticationInfo', {}).get('principal', None)
            granted = audit_log.get('data', {}).get('authorizationInfo', {}).get('granted', None)
            operation = audit_log.get('data', {}).get('authorizationInfo', {}).get('operation', None)
            resource_name = audit_log.get('data', {}).get('authorizationInfo', {}).get('resourceName', None)
            
            # Filter for failed access attempts on sensitive resources
            if granted is False and resource_name and isinstance(resource_name, str) and sensitive_resource_prefix.lower() in resource_name.lower():
                # Optional: Further filter for specific operations like 'Read' or 'Describe'
                if operation in ['Read', 'Describe']:  # Add more operations as needed
                    print(f"Detected failed access attempt:")
                    print(f"Principal: {principal}")
                    print(f"Operation: {operation}")
                    print(f"Resource: {resource_name}")
                    print(f"Full Event:")
                    print(json.dumps(audit_log, indent=4))
                    print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}\n")
                    # Here, you could add alerting logic, e.g., send email or integrate with SIEM
                else:
                    print(f"Non-matching operation for failed attempt: {operation}")
            else:
                print(f"Non-matching event (granted: {granted}, resource: {resource_name})")
                print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}\n")
        except json.JSONDecodeError:
            print(f"Failed to parse JSON: {message_value}")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()