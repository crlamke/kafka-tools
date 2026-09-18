#!/usr/bin/env python3
"""
kafka_consumer.py

Basic Kafka consumer for testing a broker deployment (e.g. the container
started by deploy_kafka.sh). Connects to a topic and prints each message as
it arrives, along with partition/offset, until stopped with Ctrl-C.

Examples:
    python3 kafka_consumer.py --topic test-topic

    # Start reading from the beginning of the topic instead of only new messages
    python3 kafka_consumer.py --topic test-topic --from-beginning

    # Use a named consumer group (offsets are tracked/committed for the group)
    python3 kafka_consumer.py --topic test-topic --group my-test-group
"""

import argparse
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer


def parse_args():
    parser = argparse.ArgumentParser(description="Basic Kafka test consumer")
    parser.add_argument("--bootstrap-server", default="localhost:9092",
                         help="Kafka bootstrap server, host:port (default: localhost:9092)")
    parser.add_argument("--topic", required=True,
                         help="Topic to consume messages from")
    parser.add_argument("--group", default=None,
                         help="Consumer group ID. If omitted, no offsets are committed "
                              "and each run behaves like a fresh, standalone consumer.")
    parser.add_argument("--from-beginning", action="store_true",
                         help="Start from the earliest available message instead of "
                              "only new messages produced after this consumer starts")

    # Optional auth flags, off by default. Present for consistency with the
    # other Kafka tooling, in case the target broker has auth enabled.
    parser.add_argument("--security-protocol", default="PLAINTEXT",
                         choices=["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"],
                         help="Kafka security protocol (default: PLAINTEXT)")
    parser.add_argument("--sasl-mechanism", default=None,
                         choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"],
                         help="SASL mechanism, if using SASL_PLAINTEXT or SASL_SSL")
    parser.add_argument("--sasl-username", default=None)
    parser.add_argument("--sasl-password", default=None)

    return parser.parse_args()


def build_consumer(args):
    kwargs = {
        "bootstrap_servers": args.bootstrap_server,
        "security_protocol": args.security_protocol,
        "group_id": args.group,
        "auto_offset_reset": "earliest" if args.from_beginning else "latest",
        "value_deserializer": lambda v: v.decode("utf-8", errors="replace"),
        "key_deserializer": lambda k: k.decode("utf-8", errors="replace") if k else None,
    }
    if args.sasl_mechanism:
        kwargs["sasl_mechanism"] = args.sasl_mechanism
        kwargs["sasl_plain_username"] = args.sasl_username
        kwargs["sasl_plain_password"] = args.sasl_password

    return KafkaConsumer(args.topic, **kwargs)


def main():
    args = parse_args()
    consumer = build_consumer(args)

    print(f"Listening on '{args.topic}' at {args.bootstrap_server} "
          f"(group={args.group or 'none'}, "
          f"start={'earliest' if args.from_beginning else 'latest'}). Ctrl-C to stop.")

    try:
        for message in consumer:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            key_part = f" key={message.key}" if message.key is not None else ""
            print(f"[{timestamp}] partition={message.partition} offset={message.offset}"
                  f"{key_part}: {message.value}")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
