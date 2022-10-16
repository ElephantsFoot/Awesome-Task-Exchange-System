import asyncio
import time
from datetime import datetime

import requests
from fastapi import FastAPI, Request
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from .kafka_adapter import consume
from .models import Base, BillingCycle, AuditLog, User, get_current_billing_cycle

app = FastAPI()
loop = asyncio.get_event_loop()

app.db = create_engine("sqlite:////tmp/ates_billing.db", echo=True, future=True)
Base.metadata.create_all(app.db)


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
    with Session(app.db) as session:
        billing_cycle = BillingCycle(
            start_datetime=datetime.now(),
        )
        session.add(billing_cycle)
        session.commit()
    loop.create_task(consume(app))


@app.get("/my_balance")
async def balance(request: Request):
    session = Session(app.db)
    stmt = select(User).where(User.public_id == request.state.current_user_public_id)
    user = session.scalars(stmt).one()

    current_billing_cycle = get_current_billing_cycle(session)

    total = session.query(
        func.sum(AuditLog.credit - AuditLog.debit)
    ).filter(AuditLog.user_id == user.id).filter(
        AuditLog.billing_cycle_id == current_billing_cycle.id
    ).scalar()

    stmt = select(AuditLog).where(AuditLog.user_id == user.id).where(AuditLog.billing_cycle_id == current_billing_cycle.id)
    audit_logs = session.scalars(stmt).all()
    return {
        "total": total,
        "audit_logs": [
            {
                "credit": audit_log.credit,
                "debit": audit_log.debit,
                "description": audit_log.description,
            } for audit_log in audit_logs
        ]
    }
