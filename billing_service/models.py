import enum
from uuid import uuid4

from sqlalchemy import Column, Integer, DateTime, select
from sqlalchemy import Enum
from sqlalchemy import String, Text, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, validates, Session

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
    fullname = Column(String)

    tasks = relationship(
        "Task", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"User(id={self.public_id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Task(Base):
    __tablename__ = "task"
    id = Column(Integer, primary_key=True, autoincrement=True)
    public_id = Column(String(32), unique=True, default=lambda: uuid4().hex)
    title = Column(String, nullable=False)
    jira_id = Column(String)
    description = Column(Text, nullable=False)
    closed = Column(Boolean, default=False)
    user_id = Column(String, ForeignKey(User.id), nullable=False)
    price = Column(Integer, nullable=False)
    reward = Column(Integer, nullable=False)

    user = relationship("User", back_populates="tasks")

    def __repr__(self):
        return f"Task(id={self.public_id!r}, title={self.title!r}, description={self.description!r})"


class BillingCycle(Base):
    __tablename__ = "billing_cycle"
    id = Column(Integer, primary_key=True, autoincrement=True)
    start_datetime = Column(DateTime, nullable=False)
    end_datetime = Column(DateTime)

    def __repr__(self):
        return f"Task(id={self.public_id!r}, title={self.title!r}, description={self.description!r})"


class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, ForeignKey(User.id), nullable=False)
    billing_cycle_id = Column(String, ForeignKey(BillingCycle.id), nullable=False)
    description = Column(Text, nullable=False)
    debit = Column(Integer, nullable=False, default=0)
    credit = Column(Integer, nullable=False, default=0)

    user = relationship("User")
    billing_cycle = relationship("BillingCycle")

    @validates('debit')
    def validates_username(self, key, value):
        if self.debit:  # Field already exists
            raise ValueError('Debit cannot be modified.')

        return value

    @validates('credit')
    def validates_username(self, key, value):
        if self.credit:  # Field already exists
            raise ValueError('Credit cannot be modified.')

        return value

    def __repr__(self):
        return f"AuditLog(description={self.description!r}, debit={self.debit!r}, credit={self.credit!r})"


def get_current_billing_cycle(session) -> BillingCycle:
    stmt = select(BillingCycle).order_by(BillingCycle.start_datetime)
    return session.scalars(stmt).first()
