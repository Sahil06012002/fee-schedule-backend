from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy setup
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()

class Database:
    def __init__(self):
        self.db = SessionLocal()

    def close(self):
        self.db.close()

    def execute(self, query, params=None):
        if params is None:
            result = self.db.execute(query)
        else:
            result = self.db.execute(query, params)
        self.db.commit()
        return result
    def begin(self):
        return self.db.begin()


def get_db():
    db = Database()
    try:
        yield db
    finally:
        db.close()