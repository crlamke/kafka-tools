#!/usr/bin/env python3
"""
kafka_producer.py

Basic Kafka producer for testing a broker deployment (e.g. the container
started by deploy_kafka.sh). Sends either a single message or reads lines
from stdin/an interactive prompt and sends each as its own message.

Examples:
    # Send one message and exit
    python3 kafka_producer.py --topic test-topic --message "hello kafka"

    # Interactive mode: type messages, one per line, Ctrl-D to quit
    python3 kafka_producer.py --topic test-topic

    # Pipe messages in from another command
    tail -f app.log | python3 kafka_producer.py --topic test-topic
"""

import argparse
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


def parse_args():
    parser = argparse.ArgumentParser(description="Basic Kafka test producer")
    parser.add_argument("--bootstrap-server", default="localhost:9092",
                         help="Kafka bootstrap server, host:port (default: localhost:9092)")
    parser.add_argument("--topic", required=True,
                         help="Topic to produce messages to")
    parser.add_argument("--message",
                         help="Send this single message and exit. If omitted, "
                              "reads messages from stdin/interactively, one per line.")
    parser.add_argument("--key",
                         help="Optional message key to use for every message sent this run")

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


def build_producer(args):
    kwargs = {
        "bootstrap_servers": args.bootstrap_server,
        "security_protocol": args.security_protocol,
        "value_serializer": lambda v: v.encode("utf-8"),
        "key_serializer": (lambda k: k.encode("utf-8")) if args.key else None,
    }
    if args.sasl_mechanism:
        kwargs["sasl_mechanism"] = args.sasl_mechanism
        kwargs["sasl_plain_username"] = args.sasl_username
        kwargs["sasl_plain_password"] = args.sasl_password

    return KafkaProducer(**kwargs)


def send_message(producer, topic, message, key=None):
    future = producer.send(topic, value=message, key=key)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        print(f"Failed to send message: {e}", file=sys.stderr)
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[{timestamp}] Sent to {record_metadata.topic} "
          f"partition={record_metadata.partition} offset={record_metadata.offset}: {message!r}")
    return True


def main():
    args = parse_args()
    producer = build_producer(args)

    try:
        if args.message is not None:
            send_message(producer, args.topic, args.message, key=args.key)
        else:
            print(f"Connected. Type messages to send to '{args.topic}' "
                  f"(Ctrl-D to quit):")
            for line in sys.stdin:
                line = line.rstrip("\n")
                if line == "":
                    continue
                send_message(producer, args.topic, line, key=args.key)
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
