from kafka import KafkaConsumer
import json
import psycopg2

# Load database credentials
with open("/home/idoko/PycharmProjects/Kafka-stram-data/package.json") as f:
    secret = json.load(f)

db_user = secret["DB_USER"]
db_password = secret["DB_PASS"]
db_name = secret["DB_NAME"]
db_host = secret["DB_HOST"]

if __name__ == "__main__":
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port="5432"
    )
    cur = conn.cursor()
    print("Connected to PostgreSQL")

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS server_data (
            ip varchar(255) NOT NULL,
            timestamp varchar(255) NOT NULL,
            application varchar(255) NOT NULL,
            client_ip varchar(255) NOT NULL,
            server_ip varchar(255) NOT NULL,
            server_latlon varchar(255) NOT NULL,
            latlon varchar(255) NOT NULL,
            ip6 varchar(255) NOT NULL,
            incomingPkts varchar(255) NOT NULL,
            outgoingPkts varchar(255) NOT NULL,
            internalpkts varchar(255) NOT NULL,
            totalPkts varchar(255) NOT NULL,
            totalExternalPkts varchar(255) NOT NULL,
            incomingBytes varchar(255) NOT NULL,
            internalBytes varchar(255) NOT NULL,
            outgoingBytes varchar(255) NOT NULL,
            totalBytes varchar(255) NOT NULL,
            totalExternalBytes varchar(255) NOT NULL
        )
    """)
    conn.commit()

    # Configure Kafka consumer
    consumer = KafkaConsumer(
        "mec-xdr",
        bootstrap_servers=['localhost:9092'],
        max_poll_records=1000,
        value_deserializer=lambda x: json.loads(x.decode("ascii")),
        auto_offset_reset="earliest"
    )

    # Consume messages and insert into PostgreSQL
    for message in consumer:
        message_data = json.loads(message.value)
        if message_data.get("ip"):
            print(message_data)  # Debugging: Print received data
            cur.execute("""
                INSERT INTO server_data (
                    ip, timestamp, application, client_ip, server_ip, server_latlon,
                    latlon, ip6, incomingPkts, outgoingPkts, internalpkts, totalPkts,
                    totalExternalPkts, incomingBytes, internalBytes, outgoingBytes,
                    totalBytes, totalExternalBytes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                message_data.get("ip"),
                message_data.get("timestamp"),
                message_data.get("application"),
                message_data.get("client_ip"),
                message_data.get("server_ip"),
                message_data.get("server_latlon"),
                message_data.get("latlon"),
                message_data.get("ip6"),
                message_data.get("incomingPkts"),
                message_data.get("outgoingPkts"),
                message_data.get("internalpkts"),
                message_data.get("totalPkts"),
                message_data.get("totalExternalPkts"),
                message_data.get("incomingBytes"),
                message_data.get("internalBytes"),
                message_data.get("outgoingBytes"),
                message_data.get("totalBytes"),
                message_data.get("totalExternalBytes")
            ))
            conn.commit()
        else:
            print("Skipping record with missing IP:", message_data)
