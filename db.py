import asyncpg
import os
import asyncio
import logging
from fastapi import HTTPException 

pool = None
pool_lock = asyncio.Lock()

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('my_logger')

async def init_db_pool():
    global pool
    host = os.getenv("host")
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    sslmode = os.getenv("sslmode")
    pool = await asyncpg.create_pool(
        host=host,
        database=dbname,
        user=user,
        password=password,
        ssl=sslmode,
        command_timeout=60,
        min_size=5,  # Minimum number of connections in the pool
        max_size=20, # Maximum number of connections in the pool
        max_inactive_connection_lifetime=300 # Connections inactive for 5 mins are closed
    )
    logger.info("Database connection pool initialized with min_size=5, max_size=20.")

async def get_db():
    global pool
    try:
        if pool is None:
            async with pool_lock:
                if pool is None:
                    await init_db_pool()
        async with pool.acquire() as connection:
            yield connection
    except Exception as e:
        logger.error(f"Failed to acquire database connection: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")


async def close_db():
    global pool
    if pool: # Check if pool exists before trying to close
        await pool.close()
        logger.info("Database connection pool closed.")
    pool = None
