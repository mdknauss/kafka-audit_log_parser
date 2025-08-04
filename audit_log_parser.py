from confluent_kafka import Consumer, KafkaError
import json

conf = {
'bootstrap.servers': 'pkc-4ywp7.us-west-2.aws.confluent.cloud:9092', # Your cluster’s bootstrap server
'security.protocol': 'SASL_SSL',
'sasl.mechanism': 'PLAIN',
'sasl.username': '<yourNameHere>',
'sasl.password': '<yourPasswordHere>',
'group.id': '<yourGroupIdHere>'
, 'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['confluent-audit-log-events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        try:
            audit_event = json.loads(msg.value().decode('utf-8'))
            print(f"Audit Event: {json.dumps(audit_event, indent=2)}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()


