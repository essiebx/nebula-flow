"""
kafka_producer_clickstream.py
PURPOSE : Generate synthetic retail clickstream events using Faker
          and publish them to the 'retail.clickstream' Kafka topic.

WHAT IT SIMULATES:
  - Real users browsing a retail website
  - Events: page_view → add_to_cart → purchase (realistic funnel)
  - ~500 events published per run by default (configurable)

HOW IT CONNECTS TO THE PIPELINE:
  Faker → JSON message → Kafka topic (retail.clickstream)
                                  ↓
                       kafka_consumer_snowpipe.py
                                  ↓
                       Snowpipe → BRONZE.raw_clickstream

USAGE:
  python ingestion/streaming/kafka_producer_clickstream.py
  python ingestion/streaming/kafka_producer_clickstream.py --events 1000
  python ingestion/streaming/kafka_producer_clickstream.py --continuous
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────
TOPIC = "retail.clickstream"

# TPC-DS item SK range (SF10 has ~300K items, we sample a subset)
ITEM_ID_MIN = 1
ITEM_ID_MAX = 300_000

# TPC-DS customer SK range
CUSTOMER_ID_MIN = 1
CUSTOMER_ID_MAX = 65_000

# Clickstream funnel probabilities
# Most sessions = page views; fewer add to cart; fewest purchase
EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.70, 0.20, 0.10]

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.50, 0.38, 0.12]

REFERRER_SOURCES = ["organic", "paid_search", "email", "social", "direct"]
REFERRER_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create and return a KafkaProducer.
    - value_serializer : converts dict → JSON bytes automatically
    - acks='all'       : wait for all replicas to acknowledge
                         (ensures no message loss)
    - retries=3        : retry on transient failures
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )


def generate_session_events(fake: Faker, session_id: str, user_id: int) -> list[dict]:
    """
    Generate 1–5 events for a single browser session.
    Events are ordered chronologically and follow a realistic
    funnel: a purchase always follows an add_to_cart.
    """
    session_events = []

    # Pick a random item the user is looking at in this session
    item_id = random.randint(ITEM_ID_MIN, ITEM_ID_MAX)

    # Session start time — within the last 7 days
    session_start = fake.date_time_between(
        start_date="-7d",
        end_date="now",
        tzinfo=timezone.utc
    )

    # Number of events in this session (1 to 5)
    n_events = random.randint(1, 5)

    device = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0]
    referrer = random.choices(REFERRER_SOURCES, weights=REFERRER_WEIGHTS)[0]

    for i in range(n_events):
        # Each event happens a few seconds after the previous one
        event_time = session_start.replace(
            second=session_start.second + (i * random.randint(5, 120))
        )

        # First event is always a page_view
        if i == 0:
            event_type = "page_view"
        else:
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

        event = {
            "session_id": session_id,
            "user_id": user_id,
            "item_id": item_id,
            "event_type": event_type,
            "event_timestamp": event_time.strftime("%Y-%m-%d %Human:%M:%S"),
            "device_type": device,
            "referrer_source": referrer,
        }
        # Fix the timestamp format (isoformat without microseconds)
        event["event_timestamp"] = event_time.strftime("%Y-%m-%d %H:%M:%S")
        session_events.append(event)

    return session_events


def on_send_success(record_metadata):
    """Callback: log successful message delivery."""
    logger.debug(
        "Message delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc):
    """Callback: log failed message delivery."""
    logger.error("Message delivery failed: %s", exc)


def produce_events(
    bootstrap_servers: str,
    n_events: int = 500,
    continuous: bool = False,
    delay_seconds: float = 0.01,
):
    """
    Produce clickstream events to Kafka.

    Args:
        bootstrap_servers : Kafka broker address (e.g. localhost:9092)
        n_events          : number of events to produce (batch mode)
        continuous        : if True, produce indefinitely (stream mode)
        delay_seconds     : pause between messages (throttle)
    """
    fake = Faker()
    producer = build_producer(bootstrap_servers)

    logger.info(
        "Starting producer → topic=%s broker=%s mode=%s",
        TOPIC,
        bootstrap_servers,
        "continuous" if continuous else f"batch ({n_events} events)",
    )

    produced = 0
    try:
        while True:
            # Simulate a user session (1–5 events)
            session_id = str(uuid.uuid4())
            user_id = random.randint(CUSTOMER_ID_MIN, CUSTOMER_ID_MAX)
            events = generate_session_events(fake, session_id, user_id)

            for event in events:
                # Use session_id as the Kafka message key so all events
                # from the same session land in the same partition
                # (preserves ordering within a session)
                producer.send(
                    TOPIC,
                    key=session_id.encode("utf-8"),
                    value=event,
                ).add_callback(on_send_success).add_errback(on_send_error)

                produced += 1
                time.sleep(delay_seconds)

                if produced % 100 == 0:
                    logger.info("Produced %d events so far...", produced)

            if not continuous and produced >= n_events:
                break

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        # Flush ensures all buffered messages are sent before shutdown
        producer.flush()
        producer.close()
        logger.info("Producer closed. Total events produced: %d", produced)


if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser(description="Retail clickstream Kafka producer")
    parser.add_argument(
        "--events", type=int, default=500,
        help="Number of events to produce in batch mode (default: 500)"
    )
    parser.add_argument(
        "--continuous", action="store_true",
        help="Run continuously until interrupted (Ctrl+C)"
    )
    parser.add_argument(
        "--delay", type=float, default=0.01,
        help="Delay in seconds between messages (default: 0.01)"
    )
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    produce_events(
        bootstrap_servers=bootstrap,
        n_events=args.events,
        continuous=args.continuous,
        delay_seconds=args.delay,
    )
