import os
import json
import uuid
import logging
import threading
from datetime import datetime
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from fastapi.encoders import jsonable_encoder

from schemas import MovieEvent, UserEvent, PaymentEvent, EventResponse, Event, HealthResponse

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("events-service")

# --- Конфиг ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")

# --- FastAPI ---
app = FastAPI(title="CinemaAbyss Events Service")

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# --- Kafka Consumer (в отдельном потоке для логирования) ---
def consume_events():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="events-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    for message in consumer:
        logger.info(f"==> Consumed event from Kafka: {message.value}")

threading.Thread(target=consume_events, daemon=True).start()


# --- Вспомогательная функция ---
def produce_event(event_type: str, payload: dict) -> EventResponse:
    event_id = f"{event_type}-{uuid.uuid4()}"
    event = Event(
        id=event_id,
        type=event_type,
        timestamp=datetime.utcnow(),
        payload=payload,
    )

    try:
        event_data = jsonable_encoder(event)
        future = producer.send(TOPIC, event_data)
        record_metadata = future.get(timeout=10)

        logger.info(f"<== Produced event: {event.dict()} → "
                    f"partition={record_metadata.partition}, offset={record_metadata.offset}")

        return EventResponse(
            status="success",
            partition=record_metadata.partition,
            offset=record_metadata.offset,
            event=event,
        )
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Kafka produce failed")


# --- API ---
@app.get("/api/events/health", response_model=HealthResponse, tags=["health"])
def health():
    return {"status": True}


@app.post("/api/events/movie", response_model=EventResponse, tags=["events"], status_code=201)
def create_movie_event(event: MovieEvent):
    return produce_event("movie", event.dict())


@app.post("/api/events/user", response_model=EventResponse, tags=["events"], status_code=201)
def create_user_event(event: UserEvent):
    return produce_event("user", event.dict())


@app.post("/api/events/payment", response_model=EventResponse, tags=["events"], status_code=201)
def create_payment_event(event: PaymentEvent):
    return produce_event("payment", event.dict())
