#Flask App for Sending Clickstream Data to Kafka
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import uuid
from datetime import datetime

app = Flask(__name__)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/')
def home():
    return "Clickstream API is running!"

@app.route('/click', methods=['POST'])
def click_event():
    try:
        # Parse data from frontend (Lovable)
        data = request.json

        # Construct a click event with UUID and current timestamp
        click_event = {
            "id": str(uuid.uuid4()),
            "user_id": data.get("user_id"),
            "event_type": data.get("event_type", "click"),
            "product_id": data.get("product_id"),
            "product_name": data.get("product_name"),
            "product_category": data.get("product_category"),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        # Send to Kafka topic
        producer.send('clickstream-topic', click_event)
        producer.flush()

        return jsonify({"status": "success", "message": "Clickstream event sent", "data": click_event}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)

