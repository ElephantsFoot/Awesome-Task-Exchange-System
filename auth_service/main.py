from hashlib import md5
from typing import Optional, Union
from uuid import uuid4

import jwt
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from .kafka_adapter import publish_event
from .models import Base, User
from schema_registry.compiled.user.v1_pb2 import User as UserEvent


app = FastAPI()

app.db = create_engine("sqlite:////tmp/ates_users.db", echo=True, future=True)
Base.metadata.create_all(app.db)


class UserSchema(BaseModel):
    fullname: str
    password: str
    email: str
    role: Optional[str]


AUTHSECRET = 'VERYSECRET^^^'


@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    session = Session(app.db)
    stmt = select(User).where(User.email == form_data.username)
    user = session.scalars(stmt).one()
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    if not md5(form_data.password.encode()).hexdigest() == user.password_hash:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    encoded_jwt = jwt.encode({"public_id": user.public_id}, AUTHSECRET, algorithm='HS256')

    return {"access_token": encoded_jwt, "token_type": "bearer"}


@app.post("/verify")
async def verify(authorization: Union[str, None] = Header(default=None)):
    token = authorization.replace("Bearer ", "")
    decoded = jwt.decode(token, AUTHSECRET, algorithms=['HS256'])
    return decoded


@app.post("/user/")
async def create_user(user: UserSchema):
    with Session(app.db) as session:
        new_user = User(
            fullname=user.fullname,
            password_hash=md5(user.password.encode()).hexdigest(),
            email=user.email,
            public_id=uuid4().hex
        )
        session.add(new_user)
        session.commit()
        user_event = UserEvent()
        user_event.role = new_user.role.name
        user_event.fullname = new_user.fullname
        user_event.email = new_user.email
        user_event.public_id = new_user.public_id
        publish_event(user_event.SerializeToString())
        return {"username": new_user.public_id}
