import time
from datetime import datetime
import json
import random

import psycopg2
import logging
import logging.config
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_status

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Voting")

config = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(config | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(config)

if __name__ == "__main__":
    logger.warning("Starting the process of voting....")

    conn = psycopg2.connect("host=localhost dbname=votedb user=postgres password=root")
    curr = conn.cursor()

    logger.warning("Connection to database voting successfull.....")

    candidates = curr.execute(
        """
        SELECT row_to_json(cols)
        FROM(
            SELECT * FROM candidates 
            ) cols;
            """)
    candidate = [candidate[0] for candidate in curr.fetchall()]

    if len(candidate) == 0:
        raise Exception("No candidates found in the database...")
    else:
        print(candidate)

    consumer.subscribe(['voter_topic'])

    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            elif message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(message.error())
                    break
            else:
                voter = json.loads(message.value().decode('utf-8'))
                chosen_candidate = random.choice(candidate)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }

                try:
                    print(f'User {vote[f'voter_id']} is voting for candidate {vote['candidate_id']}')
                    curr.execute("""
                        INSERT INTO votes(voter_id, candidate_id, voting_time)
                        VALUES(%s, %s, %s) 
                    """,
                                 (
                                    vote['voter_id'], vote['candidate_id'], vote['voting_time']
                                 ))
                    conn.commit()

                    producer.produce(
                        'voter_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_status
                    )
                    producer.poll(0)

                except Exception as e:
                    print("Error", e)
            time.sleep(0.5)
    except Exception as e:
        print(e)



