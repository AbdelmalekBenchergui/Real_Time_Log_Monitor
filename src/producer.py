import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import json

fake = Faker()

def generate_log():
    log_levels = ['INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL','SUCCESS']
    apis = [
        "https://api.weathernow",
        "https://api.foodie",
        "https://api.bookfinder",
        "https://api.musicstream",
        "https://api.travelbuddy",
        "https://api.sportsinfo",
        "https://api.moviehub",
        "https://api.healthtracker",
        "https://api.financenow",
        "https://api.gamestats",
        "https://api.educate",
        "https://api.socialconnect",
        "https://api.technews",
        "https://api.petcare",
        "https://api.smartshopping",
        "https://api.homeautomation",
        "https://api.jobsearch",
        "https://api.codinghub",
        "https://api.languagelearn",
        "https://api.tradingguru"
    ]

    status_codes = [200, 201, 202, 301, 302, 400, 401, 403, 404, 422, 500, 502, 503, 504]
    response_codes = [200, 400, 500, 401, 402, 429, 503, 504]

    timestamp = int(time.time() * 1000)
    timestamp_human = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
    
    var = {
        'id': str(fake.uuid4()),
        'ip_address': fake.ipv4(),
        'timestamp': timestamp,
        'timestamp_human': timestamp_human,
        'api': random.choice(apis),
        'method': random.choice(["GET", "POST", "PUT", "DELETE"]),
        'response_time': random.randint(50, 1000),
        'status_code': random.choice(status_codes),
        'log_level': random.choices(log_levels, weights=[10,10,10,10,10,50], k=1)[0],
        'response_code': random.choice(response_codes),
    }

    return var

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

try:
    while True:
        log = generate_log()
        producer.send("logs", log)
        print("Sent:", log)
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Stopping producer...")
    producer.flush()
    producer.close()
