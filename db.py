import asyncpg
import os
import asyncio
import logging
import traceback
from fastapi import HTTPException 

pool = None
pool_lock = asyncio.Lock()

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('my_logger')

async def init_db_pool():
    global pool
    try:
        host = os.getenv("host")
        dbname = os.getenv("dbname")
        user = os.getenv("user")
        password = os.getenv("password")
        sslmode = os.getenv("sslmode")
        
        # Log connection parameters (without password)
        logger.info(f"Attempting to connect to database: host={host}, dbname={dbname}, user={user}, sslmode={sslmode}")
        
        # Check if required environment variables are set
        if not all([host, dbname, user, password]):
            missing = []
            if not host: missing.append("host")
            if not dbname: missing.append("dbname")
            if not user: missing.append("user")
            if not password: missing.append("password")
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            raise ValueError(f"Missing required database configuration: {', '.join(missing)}")
        
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
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        pool = None
        raise

async def get_db():
    global pool
    try:
        if pool is None:
            async with pool_lock:
                if pool is None:
                    await init_db_pool()
        
        # After initialization, check if pool is still None (in case init failed)
        if pool is None:
            logger.error("Database pool initialization failed - pool is still None")
            raise HTTPException(status_code=500, detail="Database pool not initialized")
            
        async with pool.acquire() as connection:
            yield connection
    except asyncpg.exceptions.TooManyConnectionsError as e:
        logger.error(f"Too many connections error: {e}")
        raise HTTPException(status_code=503, detail="Database connection pool exhausted")
    except Exception as e:
        logger.error(f"Failed to acquire database connection: {e}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")


async def close_db():
    global pool
    if pool: # Check if pool exists before trying to close
        await pool.close()
        logger.info("Database connection pool closed.")
    pool = None
