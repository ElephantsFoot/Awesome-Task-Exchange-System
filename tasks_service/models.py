import enum
from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import String, Text, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class UserRole(enum.Enum):
    DEVELOPER = 1
    ADMIN = 2
    MANAGER = 3


class User(Base):
    __tablename__ = "user_account"
    public_id = Column(String(32), primary_key=True, unique=True)
    role = Column(Enum(UserRole), default=UserRole.DEVELOPER)
    fullname = Column(String)

    tasks = relationship(
        "Task", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"User(id={self.public_id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Task(Base):
    __tablename__ = "task"
    public_id = Column(String(32), primary_key=True, unique=True, default=lambda: uuid4().hex)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    closed = Column(Boolean, default=False)
    user_id = Column(String, ForeignKey(User.public_id), nullable=False)

    user = relationship("User", back_populates="tasks")

    def __repr__(self):
        return f"Task(id={self.public_id!r}, title={self.title!r}, description={self.description!r})"
