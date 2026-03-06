"""
kafka_consumer_snowpipe.py
PURPOSE : Consume clickstream messages from the Kafka topic
          'retail.clickstream', batch them into NDJSON files,
          stage them to Snowflake internal stage, then trigger
          Snowpipe via the REST API for auto-ingestion.

FLOW:
  Kafka topic (retail.clickstream)
       ↓  consume batch
  In-memory buffer (BATCH_SIZE messages)
       ↓  when full or FLUSH_INTERVAL_SECONDS elapsed
  PUT file → @BRONZE.clickstream_stage (Snowflake internal stage)
       ↓
  Snowpipe REST API insertFiles call
       ↓
  Snowpipe ingests file → BRONZE.raw_clickstream

USAGE:
  python ingestion/streaming/kafka_consumer_snowpipe.py
"""

import json
import logging
import os
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
TOPIC = "retail.clickstream"
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP_ID", "snowpipe-consumer-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Batch settings: collect this many messages before staging
BATCH_SIZE = 100

# Flush interval: stage whatever we have after this many seconds
# even if BATCH_SIZE hasn't been reached (prevents stale data)
FLUSH_INTERVAL_SECONDS = 30

# Snowflake stage path
STAGE_NAME = "@RETAIL_DB.BRONZE.clickstream_stage"


def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables.
    We use the connector directly here (not dbt/Airflow) because
    this consumer runs as a standalone long-lived process.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="RETAIL_DB",
        schema="BRONZE",
        role=os.getenv("SNOWFLAKE_ROLE", "ENGINEER_ROLE"),
    )


def stage_and_ingest(messages: list[dict], conn) -> bool:
    """
    Write messages to a temp NDJSON file, PUT it to the Snowflake
    internal stage, then trigger Snowpipe ingestion.

    Args:
        messages : list of decoded Kafka message dicts
        conn     : active Snowflake connection

    Returns:
        True on success, False on error
    """
    if not messages:
        return True

    # Build a timestamped filename so files don't overwrite each other
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    filename = f"clickstream_batch_{ts}.ndjson"

    try:
        # Write messages to a temp file (one JSON object per line = NDJSON)
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / filename
            with open(filepath, "w") as f:
                for msg in messages:
                    f.write(json.dumps(msg) + "\n")

            logger.info(
                "Staging %d messages → %s/%s",
                len(messages), STAGE_NAME, filename
            )

            # PUT uploads the local file to the Snowflake internal stage
            # AUTO_COMPRESS=TRUE gzip-compresses the file in transit
            cursor = conn.cursor()
            put_result = cursor.execute(
                f"PUT file://{filepath} {STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=FALSE"
            ).fetchone()

            logger.info("PUT result: %s", put_result)

            # Trigger Snowpipe to ingest the staged file
            # REFRESH scans the stage for new files and queues them
            refresh_result = cursor.execute(
                "ALTER PIPE RETAIL_DB.BRONZE.clickstream_pipe REFRESH"
            ).fetchone()

            logger.info(
                "Snowpipe triggered. Queued files: %s", refresh_result
            )
            cursor.close()

        return True

    except Exception as e:
        logger.error("Failed to stage/ingest batch: %s", e)
        return False


def run_consumer():
    """
    Main consumer loop:
    1. Poll Kafka for messages
    2. Accumulate into a buffer
    3. Flush buffer to Snowflake when BATCH_SIZE or time limit reached
    4. Commit Kafka offsets only after successful staging
       (at-least-once delivery guarantee)
    """
    logger.info(
        "Starting consumer | topic=%s group=%s broker=%s",
        TOPIC, CONSUMER_GROUP, BOOTSTRAP_SERVERS
    )

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",       # start from beginning if no committed offset
        enable_auto_commit=False,           # we commit manually after successful staging
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,           # stop blocking after 5s of no messages
    )

    conn = get_snowflake_connection()

    buffer = []
    last_flush_time = time.time()

    try:
        while True:
            # Poll Kafka for messages (blocks up to consumer_timeout_ms)
            try:
                for message in consumer:
                    buffer.append(message.value)

                    # Check if we should flush
                    time_since_flush = time.time() - last_flush_time
                    should_flush = (
                        len(buffer) >= BATCH_SIZE
                        or time_since_flush >= FLUSH_INTERVAL_SECONDS
                    )

                    if should_flush and buffer:
                        success = stage_and_ingest(buffer, conn)
                        if success:
                            # Only commit offsets if staging succeeded
                            # This ensures we don't lose messages on failure
                            consumer.commit()
                            logger.info(
                                "Committed offsets for %d messages", len(buffer)
                            )
                            buffer = []
                            last_flush_time = time.time()
                        else:
                            logger.warning(
                                "Staging failed — retaining %d messages in buffer",
                                len(buffer)
                            )

            except StopIteration:
                # consumer_timeout_ms elapsed with no new messages
                # Flush whatever is in the buffer
                if buffer:
                    logger.info(
                        "Timeout flush: %d buffered messages", len(buffer)
                    )
                    success = stage_and_ingest(buffer, conn)
                    if success:
                        consumer.commit()
                        buffer = []
                        last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumer shutting down...")
        # Final flush before exit
        if buffer:
            stage_and_ingest(buffer, conn)
            consumer.commit()
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    run_consumer()
