import json
from random import randrange

import aiokafka as aiokafka
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import UserRole, User, Task, AuditLog, get_current_billing_cycle
from schema_registry.compiled.user.v1_pb2 import User as UserEvent
from schema_registry.compiled.task.v1_pb2 import Task as TaskEvent
from schema_registry.compiled.task_assigned.v1_pb2 import TaskAssigned
from schema_registry.compiled.task_closed.v1_pb2 import TaskClosed


async def consume(app):
    consumer = aiokafka.AIOKafkaConsumer(
        "users-stream", "tasks-stream", "tasks-lifecycle",
        bootstrap_servers='localhost:9092',
        group_id="billing_service",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
            if msg.topic == "users-stream":
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
            elif msg.topic == "tasks-stream":
                task = TaskEvent()
                task.ParseFromString(msg.value)
                with Session(app.db) as session:
                    stmt = select(User).where(User.public_id == task.user_id)
                    user = session.scalars(stmt).one()
                    new_task = Task(
                        public_id=task.task_id,
                        jira_id=task.jira_id,
                        title=task.title,
                        description=task.description,
                        closed=task.closed,
                        user=user,
                        price=randrange(10, 20),
                        reward=randrange(20, 40),
                    )
                    session.add(new_task)
                    session.commit()
            elif msg.topic == "tasks-lifecycle":
                if dict(msg.headers)['MessageName'] == b'task_assigned':
                    task_assigned = TaskAssigned()
                    task_assigned.ParseFromString(msg.value)
                    with Session(app.db) as session:
                        stmt = select(User).where(User.public_id == task_assigned.user_id)
                        user = session.scalars(stmt).one()
                        stmt = select(Task).where(Task.public_id == task_assigned.task_id)
                        task = session.scalars(stmt).one()
                        new_audit_log = AuditLog(
                            description='{} assigned'.format(task.jira_id),
                            debit=task.price,
                            credit=0,
                            user=user,
                            billing_cycle=get_current_billing_cycle(session),
                        )
                        session.add(new_audit_log)
                        session.commit()
                elif dict(msg.headers)['MessageName'] == b'task_closed':
                    task_closed = TaskClosed()
                    task_closed.ParseFromString(msg.value)
                    with Session(app.db) as session:
                        stmt = select(User).where(User.public_id == task_closed.user_id)
                        user = session.scalars(stmt).one()
                        stmt = select(Task).where(Task.public_id == task_closed.task_id)
                        task = session.scalars(stmt).one()
                        new_audit_log = AuditLog(
                            description='{} closed'.format(task.jira_id),
                            debit=0,
                            credit=task.reward,
                            user=user,
                            billing_cycle=get_current_billing_cycle(session),
                        )
                        session.add(new_audit_log)
                        session.commit()
    finally:
        await consumer.stop()


producer = AIOKafkaProducer(
    bootstrap_servers='localhost:9092',
)


async def publish_event(serialized_task):
    await producer.start()
    try:
        await producer.send_and_wait("tasks_stream", serialized_task)
    finally:
        await producer.stop()
