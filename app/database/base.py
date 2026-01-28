from sqlalchemy import Table, MetaData
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, declarative_base

from app.core.config import settings

Base: DeclarativeBase = declarative_base()

engine = create_async_engine(settings.DATABASE_URL)
metadata = MetaData()

async def reflect_existing_table(table_name):
    async with engine.connect() as conn:
        table = await conn.run_sync(
            lambda sync_conn: Table(table_name, metadata, autoload_with=sync_conn)
        )
        return table