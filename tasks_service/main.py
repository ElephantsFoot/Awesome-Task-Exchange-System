import asyncio
import time
from typing import Union

import requests
from fastapi import FastAPI, Header, Request
from pydantic import BaseModel
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from kafka_adapter import consume, publish_event
from models import Base, User, UserRole, Task

app = FastAPI()
loop = asyncio.get_event_loop()

app.db = create_engine("sqlite:////tmp/ates_tasks.db", echo=True, future=True)
Base.metadata.create_all(app.db)


class TaskSchema(BaseModel):
    title: str
    description: str


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
    loop.create_task(consume(app))


@app.post("/task")
async def create_task(request: Request, task: TaskSchema, authorization: Union[str, None] = Header(default=None)):
    with Session(app.db) as session:
        stmt = select(User).where(User.role == UserRole.DEVELOPER).order_by(func.random())
        random_developer = session.scalars(stmt).one()

        new_task = Task(
            title=task.title,
            description=task.description,
            user=random_developer,
        )

        session.add(new_task)
        session.commit()
        await publish_event(
            {
                "task_id": new_task.public_id,
                "title": new_task.title,
                "description": new_task.description,
                "closed": new_task.closed,
                "user_id": new_task.user_id,
            },
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
        "user_id": task.user_id,
    }
