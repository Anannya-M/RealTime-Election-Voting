import json
import random

import psycopg2
import logging
import logging.config

from confluent_kafka import SerializingProducer

from data_generation import generate_data_for_candidates, generate_data_for_voters

logging.config.fileConfig("Properties/configuration/logging.config")


def create_table(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
        candidate_id VARCHAR(255) PRIMARY KEY,
        candidate_name VARCHAR(255),
        party_affiliation VARCHAR(255),
        biography TEXT,
        campaign_platform TEXT,
        photo_url TEXT)
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
        voter_id VARCHAR(255) PRIMARY KEY,
        voter_name VARCHAR(255),
        date_of_birth DATE,
        gender VARCHAR(50),
        nationality VARCHAR(255),
        registration_number VARCHAR(255),
        address_street VARCHAR(255),
        address_city VARCHAR(255),
        address_state VARCHAR(255),
        address_country VARCHAR(255),
        address_postcode VARCHAR(255),
        email VARCHAR(255),
        phone_number VARCHAR(255),
        picture TEXT, 
        registered_age INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes(
        voter_id VARCHAR(255) UNIQUE,
        candidate_id VARCHAR(255),
        voting_time TIMESTAMP,
        vote int DEFAULT 1, 
        primary key (voter_id, candidate_id)
        )
        """
    )
    conn.commit()


def delivery_status(err, msg):
    if err is not None:
        logging.error("Message delivery failed...See traces==>", err)
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def insert_voters(conn, cur, voter):
    cur.execute("""INSERT INTO voters(voter_id, voter_name, date_of_birth, gender, nationality, registration_number, 
    address_street, address_city, address_state,address_country, address_postcode, email, phone_number, picture, registered_age)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
                (
                    voter['voter_id'], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                    voter['nationality'],
                    voter['registration_number'], voter['address']['street'], voter['address']['city'],
                    voter['address']['state'],
                    voter['address']['country'], voter['address']['postcode'], voter['email'], voter['phone_number'],
                    voter['picture'], voter['registered_age']
                ))
    conn.commit()


def main():
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    try:
        logging.info("Trying to connect to the Postgres....")
        conn = psycopg2.connect("host=localhost dbname=votedb user=postgres password=root")
        curr = conn.cursor()

        logging.info("Connection is successfull....")

        logging.info("Now creating tables....")
        create_table(conn, curr)

        logging.info("Generating the data for these tables....")
        curr.execute("""
            SELECT * FROM candidates
        """)
        candidates = curr.fetchall()
        # print(candidates)

        if len(candidates) == 0:
            logging.info("Candidate table is empty.Generating data....")
            for i in range(3):
                candidate = generate_data_for_candidates(i, 3)
                curr.execute("""
                    INSERT INTO candidates(candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                    VALUES(%s, %s, %s, %s, %s, %s)
                """,
                             (
                                 candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
                                 candidate['biography'], candidate['campaign_platform'], candidate['photo_url']
                             ))
                conn.commit()
        else:
            logging.info("The candidate table already has the data....")

        curr.execute("""
                    SELECT * FROM voters
                """)
        voters = curr.fetchall()
        if len(voters) == 0:
            for i in range(1000):
                voter_data = generate_data_for_voters()
                insert_voters(conn, curr, voter_data)

                producer.produce(
                    "voter_topic",
                    key=voter_data['voter_id'],
                    value=json.dumps(voter_data),
                    on_delivery=delivery_status
                )

                print(f'Produced voter {i}/1000')
                producer.flush()
        else:
            logging.info(f"The voters table already has {len(voters)} records....")


    except Exception as e:
        logging.error("An error occured while executing the main() method.See traces ==>", str(e))
    else:
        logging.warning("main() method executed successfully...")


if __name__ == '__main__':
    logging.info("Lets get started....")
    main()
    logging.info("Application done...")
