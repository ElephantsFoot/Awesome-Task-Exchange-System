import aiokafka as aiokafka
from aiokafka import AIOKafkaProducer
from sqlalchemy.orm import Session

from schema_registry.compiled.user.v1_pb2 import User as UserEvent
from .models import UserRole, User


async def consume(app):
    consumer = aiokafka.AIOKafkaConsumer(
        "users-stream",
        bootstrap_servers='localhost:9092',
        group_id="tasks_service",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
            user = UserEvent()
            user.ParseFromString(msg.value)
            with Session(app.db) as session:
                new_user = User(
                    public_id=user.public_id,
                    role=UserRole(user.role),
                    fullname=user.fullname,
                )
                session.add(new_user)
                session.commit()
    finally:
        await consumer.stop()


async def publish_cud_event(app, serialized_task):
    await app.producer.send_and_wait("tasks-stream", serialized_task)


async def publish_lifecycle_event(app, serialized_task, headers):
    await app.producer.send_and_wait("tasks-lifecycle", serialized_task, headers=headers)
