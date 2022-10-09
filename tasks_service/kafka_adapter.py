import json

import aiokafka as aiokafka
from aiokafka import AIOKafkaProducer
from sqlalchemy.orm import Session

from models import UserRole, User


async def consume(app):
    consumer = aiokafka.AIOKafkaConsumer(
        "users-stream",
        bootstrap_servers='localhost:9092'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
            msg_dict = json.loads(msg.value.decode())
            with Session(app.db) as session:
                new_user = User(
                    public_id=msg_dict["public_id"],
                    role=UserRole[msg_dict["role"]],
                    fullname=msg_dict["fullname"],
                )
                session.add(new_user)
                session.commit()
    finally:
        await consumer.stop()


producer = AIOKafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
)


async def publish_event(task_dict):
    await producer.start()
    try:
        await producer.send_and_wait("tasks_stream", task_dict)
    finally:
        await producer.stop()
