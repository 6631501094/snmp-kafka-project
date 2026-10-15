import json
from datetime import datetime, timezone
from kafka import KafkaConsumer

# ==========================================
# Kafka Configuration
# ==========================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "snmp"
CONSUMER_GROUP = "snmp-worker-test-02"

# ==========================================
# Kafka Consumer
# ==========================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)

# ==========================================
# Display Header
# ==========================================

print("=" * 40)
print("Cisco SNMP Kafka Worker")
print("=" * 40)
print(f"Kafka Server : {KAFKA_SERVER}")
print(f"Kafka Topic  : {KAFKA_TOPIC}")
print(f"Consumer Group: {CONSUMER_GROUP}")
print("Waiting for SNMP traffic messages...")
print("=" * 40, flush=True)

# ==========================================
# Consume Kafka Messages
# ==========================================

try:
    for record in consumer:
        data = record.value

        timestamp = data.get(
            "timestamp",
            datetime.now(timezone.utc).isoformat()
        )

        router = data.get(
            "router",
            data.get("router_ip", "192.168.79.136")
        )

        interface = data.get(
            "interface",
            "FastEthernet1/1"
        )

        in_octets = int(
            data.get("ifInOctets", data.get("in_octets", 0))
        )

        out_octets = int(
            data.get("ifOutOctets", data.get("out_octets", 0))
        )

        in_mbps = float(data.get("in_mbps", 0.0))
        out_mbps = float(data.get("out_mbps", 0.0))

        print()
        print(f"Timestamp : {timestamp}")
        print(f"Router    : {router}")
        print(f"Interface : {interface}")
        print(f"IN Octets : {in_octets}")
        print(f"OUT Octets: {out_octets}")
        print(f"IN Traffic: {in_mbps:.4f} Mbps")
        print(f"OUT Traffic: {out_mbps:.4f} Mbps")
        print(f"Partition : {record.partition}")
        print(f"Offset    : {record.offset}")
        print("-" * 40, flush=True)

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    consumer.close()
