import asyncio
import re
import time
from typing import Union

import requests
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Header, Request
from pydantic import BaseModel, constr
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from .kafka_adapter import consume, publish_cud_event, publish_lifecycle_event
from .models import Base, User, UserRole, Task
from schema_registry.compiled.task.v1_pb2 import Task as TaskEvent
from schema_registry.compiled.task_assigned.v1_pb2 import TaskAssigned
from schema_registry.compiled.task_closed.v1_pb2 import TaskClosed

app = FastAPI()
loop = asyncio.get_event_loop()

app.db = create_engine("sqlite:////tmp/ates_tasks.db", echo=True, future=True)
Base.metadata.create_all(app.db)


class TaskSchema(BaseModel):
    title: constr(regex=r'^\[(.*)\] - (.*)$')
    description: str


class CloseTaskSchema(BaseModel):
    public_id: str


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = requests.post('http://127.0.0.1:8000/verify', headers=request.headers)
    request.state.current_user_public_id = response.json()['public_id']
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.on_event("startup")
async def startup_event():
    app.producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
    )
    await app.producer.start()
    loop.create_task(consume(app))


@app.on_event("shutdown")
async def shutdown_event():
    await app.producer.stop()


@app.post("/task")
async def create_task(request: Request, task: TaskSchema, authorization: Union[str, None] = Header(default=None)):
    with Session(app.db) as session:
        stmt = select(User).where(User.role == UserRole.DEVELOPER).order_by(func.random())
        random_developer = session.scalars(stmt).first()

        jira_id, task_title = re.match(r'\[(.*)\] - (.*)', task.title).groups()
        new_task = Task(
            title=task_title,
            jira_id=jira_id,
            description=task.description,
            user=random_developer,
        )

        session.add(new_task)
        session.commit()

        task_event = TaskEvent()
        task_event.task_id = new_task.public_id
        task_event.title = new_task.title
        task_event.jira_id = new_task.jira_id
        task_event.description = new_task.description
        task_event.closed = new_task.closed
        task_event.user_id = new_task.user.public_id
        await publish_cud_event(app, task_event.SerializeToString())

        task_assigned = TaskAssigned()
        task_assigned.task_id = new_task.public_id
        task_assigned.user_id = new_task.user.public_id
        await publish_lifecycle_event(
            app,
            task_assigned.SerializeToString(),
            [
                ("MessageName", b"task_assigned"),
                ("Version", b"1"),
            ],
        )

        return {"task": new_task.public_id}


@app.get("/task/{task_id}")
async def get_task(request: Request, task_id, authorization: Union[str, None] = Header(default=None)):
    session = Session(app.db)
    stmt = select(Task).where(Task.public_id == task_id)
    task = session.scalars(stmt).one()
    return {
        "task_id": task.public_id,
        "title": task.title,
        "description": task.description,
        "closed": task.closed,
        "user_id": task.user.public_id,
    }


@app.post("/close_task")
async def close_task(request: Request, data: CloseTaskSchema, authorization: Union[str, None] = Header(default=None)):
    with Session(app.db) as session:
        stmt = select(Task).where(Task.public_id == data.public_id)
        task = session.scalars(stmt).one()
        task.closed = True
        session.commit()

        task_closed = TaskClosed()
        task_closed.task_id = task.public_id
        task_closed.user_id = task.user.public_id
        await publish_lifecycle_event(
            app,
            task_closed.SerializeToString(),
            [
                ("MessageName", b"task_closed"),
                ("Version", b"1"),
            ],
        )

        return {}
