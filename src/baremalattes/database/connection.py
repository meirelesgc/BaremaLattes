from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from baremalattes.settings import Settings

settings = Settings()

engine = create_engine(settings.DATABASE_URL, future=True)

SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=Session
)


@lru_cache
def get_session():
    return SessionLocal()
