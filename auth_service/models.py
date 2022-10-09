import enum

from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class UserRole(enum.Enum):
    DEVELOPER = 1
    ADMIN = 2
    MANAGER = 3


class User(Base):
    __tablename__ = "user_account"
    id = Column(Integer, primary_key=True, autoincrement=True)
    public_id = Column(String(32), unique=True)
    role = Column(Enum(UserRole), default=UserRole.DEVELOPER)
    email = Column(String, unique=True)
    password_hash = Column(String)
    fullname = Column(String)

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"
