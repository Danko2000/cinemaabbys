# src/microservices/events/app.py
from flask import Flask, request, jsonify
import json
import os
import threading
import logging
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Переменные окружения
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events"
}

# Глобальный producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

# Функция потребителя (consumer)
def start_consumer():
    consumer = KafkaConsumer(
        *TOPICS.values(),
        bootstrap_servers=KAFKA_BROKERS,
        group_id="events-service-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Consumer started, listening for messages...")

    for msg in consumer:
        topic = msg.topic
        event = msg.value
        logger.info(f"Consumed event from topic '{topic}': {event}")

# Запуск consumer в отдельном потоке
threading.Thread(target=start_consumer, daemon=True).start()


# --- API ---
@app.route('/api/events/health', methods=['GET'])
def health():
    return jsonify({"status": True}), 200


@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    data = request.json
    if not data or "user_id" not in data or "action" not in data:
        return jsonify({"error": "Missing required fields: user_id, action"}), 400

    event = {
        "id": f"user-{data['user_id']}-{data['action']}",
        "type": "user",
        "timestamp": data.get("timestamp"),
        "payload": data
    }

    try:
        future = producer.send(TOPICS["user"], event)
        result = future.get(timeout=10)
        response = {
            "status": "success",
            "partition": result.partition,
            "offset": result.offset,
            "event": event
        }
        logger.info(f"Produced user event: {event}")
        return jsonify(response), 201
    except Exception as e:
        logger.error(f"Failed to send user event: {str(e)}")
        return jsonify({"error": "Failed to publish event"}), 500


@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    data = request.json
    if not data or "movie_id" not in data or "title" not in data or "action" not in data:
        return jsonify({"error": "Missing required fields: movie_id, title, action"}), 400

    event = {
        "id": f"movie-{data['movie_id']}-{data['action']}",
        "type": "movie",
        "timestamp": data.get("timestamp"),
        "payload": data
    }

    try:
        future = producer.send(TOPICS["movie"], event)
        result = future.get(timeout=10)
        response = {
            "status": "success",
            "partition": result.partition,
            "offset": result.offset,
            "event": event
        }
        logger.info(f"Produced movie event: {event}")
        return jsonify(response), 201
    except Exception as e:
        logger.error(f"Failed to send movie event: {str(e)}")
        return jsonify({"error": "Failed to publish event"}), 500


@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    data = request.json
    if not data or "payment_id" not in data or "status" not in data:
        return jsonify({"error": "Missing required fields: payment_id, status"}), 400

    event = {
        "id": f"payment-{data['payment_id']}-{data['status']}",
        "type": "payment",
        "timestamp": data.get("timestamp"),
        "payload": data
    }

    try:
        future = producer.send(TOPICS["payment"], event)
        result = future.get(timeout=10)
        response = {
            "status": "success",
            "partition": result.partition,
            "offset": result.offset,
            "event": event
        }
        logger.info(f"Produced payment event: {event}")
        return jsonify(response), 201
    except Exception as e:
        logger.error(f"Failed to send payment event: {str(e)}")
        return jsonify({"error": "Failed to publish event"}), 500


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8082))
    app.run(host="0.0.0.0", port=port)