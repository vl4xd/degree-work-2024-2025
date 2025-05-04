from typing import Annotated
from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import settings


async_engine = create_async_engine(
    url = settings.DATABASE_URL_asyncpg,
    echo = False,
)

sync_engine = create_engine(
    url = settings.DATABASE_URL_psycopg,
    echo = False,
)


async_session_factory = async_sessionmaker(async_engine)

sync_session_factory = sessionmaker(sync_engine)


int_3 = Annotated[int, 3]
str_50 = Annotated[str, 50]
str_100 = Annotated[str, 100]
str_200 = Annotated[str, 200]

class Base(DeclarativeBase):
    pass
