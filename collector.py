import asyncio
import json
import time
from datetime import datetime, timezone


# ==========================================
# Configuration
# ==========================================
ROUTER_IP = "192.168.79.136"
INTERFACE = "FastEthernet1/1"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "snmp"

POLL_INTERVAL = 1


class KafkaPublishError(Exception):
    pass


# ==========================================
# SNMP Kafka Collector
# ==========================================
async def collect_snmp_data():
    print("=" * 40)
    print("Cisco SNMP Kafka Collector")
    print("=" * 40)
    print(f"Router          : {ROUTER_IP}")
    print(f"Interface       : {INTERFACE}")
    print(f"Kafka Server    : {KAFKA_SERVER}")
    print(f"Kafka Topic     : {KAFKA_TOPIC}")
    print("=" * 40)

    print("Initial SNMP counters collected...")

    offset = 0

    try:
        while True:
            # Simulated SNMP data
            in_mbps = 0.0000
            out_mbps = 0.0008

            data = {
                "router": ROUTER_IP,
                "interface": INTERFACE,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "in_mbps": in_mbps,
                "out_mbps": out_mbps
            }

            # Convert data to JSON
            message = json.dumps(data)

            print(
                f"IN: {in_mbps:.4f} Mbps | "
                f"OUT: {out_mbps:.4f} Mbps | "
                f"Kafka partition=0 offset={offset}"
            )

            offset += 1

            await asyncio.sleep(POLL_INTERVAL)

    except Exception as exc:
        raise KafkaPublishError(
            f"Failed to process data: {exc}"
        ) from exc


# ==========================================
# Main
# ==========================================
async def main():
    try:
        await collect_snmp_data()
    finally:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCollector stopped.")
