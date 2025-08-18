# src/microservices/proxy/app.py
from flask import Flask, request, jsonify
import requests
import os
import random
import logging


app = Flask(__name__)
app.logger.setLevel(logging.INFO)
# Получаем переменные окружения
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

# Процент трафика, который идёт в movies-service (остальное — в монолит)
MIGRATION_PERCENT = float(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

@app.route('/api/movies', methods=['GET'])
def proxy_movies():
    # Определяем, куда направить запрос
    if random.random() * 100 < MIGRATION_PERCENT:
        target_url = f"{MOVIES_SERVICE_URL}/api/movies"
        source = "movies-service"
    else:
        target_url = f"{MONOLITH_URL}/api/movies"
        source = "monolith"

    try:
        # Проксируем запрос
        resp = requests.get(target_url, timeout=10)
        response = jsonify(resp.json())
        response.status_code = resp.status_code
        app.logger.info(f"Route: {request.url} → {source} ({target_url}) | Status: {resp.status_code}")
        return response
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request failed to {target_url}: {str(e)}")
        return jsonify({"error": "Service unavailable"}), 503

# Проксирование других эндпоинтов (если нужно)
@app.route('/api/<path:service_path>', methods=['GET', 'POST'])
def proxy_other(service_path):
    # Определяем, к какому сервису идёт запрос
    if service_path.startswith("events/"):
        base_url = EVENTS_SERVICE_URL
    elif service_path.startswith("users") or service_path.startswith("payments") or service_path.startswith("subscriptions"):
        base_url = MONOLITH_URL
    else:
        return jsonify({"error": "Service not found"}), 404

    target_url = f"{base_url}/api/{service_path}"
    try:
        if request.method == 'GET':
            resp = requests.get(target_url, params=request.args, timeout=10)
        else:
            resp = requests.post(target_url, json=request.json, timeout=10)
        response = jsonify(resp.json())
        response.status_code = resp.status_code
        return response
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request failed: {str(e)}")
        return jsonify({"error": "Service unavailable"}), 503

@app.route('/health', methods=['GET'])
def health():
    return "Strangler Fig Proxy is healthy", 200

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port)