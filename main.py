from itsdangerous import URLSafeTimedSerializer
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi import FastAPI, Depends
from psycopg2 import extras
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import aiofiles
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# UTILS
from db import init_db_pool, close_db, get_db
from superflex_models import UserDataModel, LeagueDataModel, RosterDataModel, RanksDataModel
from utils import (get_user_id, insert_current_leagues, player_manager_rosters, insert_ranks_summary)

# Load environment variables from .env file
load_dotenv()
# Define a list of allowed origins (use ["*"] for allowing all origins)
origins = [
    "*",
]

app = FastAPI()
# Add CORSMiddleware to the application instance
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

#initialize the db pool
@app.on_event("startup")
async def startup_event():
    await init_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    await close_db()


# POST ROUTES
@app.post("/user_details")
async def user_details(user_data: UserDataModel, db=Depends(get_db)):
    return await insert_current_leagues(db, user_data)


@app.post("/roster")
async def roster(roster_data: RosterDataModel, db=Depends(get_db)):
    print('attempt rosters')
    return await player_manager_rosters(db, roster_data)


@app.post("/ranks_summary")
async def ranks_summary(ranks_data: RanksDataModel, db=Depends(get_db)):
    print('attempt ranks summary')
    return await insert_ranks_summary(db, ranks_data)


# GET ROUTES
@app.get("/leagues")
async def leagues(league_year: str, user_name: str, guid: str, db=Depends(get_db)):
    # Get the user_id (ensure get_user_id is also an async function)
    user_id = await get_user_id(user_name)
    session_id = guid

    # Assemble the SQL file path
    sql_path = Path.cwd() / "sql" / "leagues" / "get_leagues.sql"
    if not sql_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read the SQL query and personalize it
    async with aiofiles.open(sql_path, mode='r') as get_leagues_file:
        get_leagues_sql = await get_leagues_file.read()
        get_leagues_sql = (get_leagues_sql
                           .replace("'session_id'", f"'{session_id}'")
                           .replace("'user_id'", f"'{user_id}'")
                           .replace("'league_year'", f"'{league_year}'"))

    # Execute the query asynchronously and fetch results
    results = await db.fetch(get_leagues_sql)
    return results


@app.get("/get_user")
async def get_user(user_name: str):
    user_id = await get_user_id(user_name)
    return {"user_id": user_id}


@app.get('/ranks')
async def ranks(platform: str, db=Depends(get_db)):
    # Ensure the SQL file exists and is readable
    sql_path = Path.cwd() / "sql" / "player_values" / "ranks" / f"{platform}.sql"
    if not sql_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")
    
    # Asynchronously read the SQL file
    async with aiofiles.open(sql_path, mode='r') as player_values_file:
        player_values_sql = await player_values_file.read()

    # Execute the query asynchronously
    result = await db.fetch(player_values_sql)
    return result


@app.get('/trade_calculator')
async def trade_calculator(platform: str, rank_type: str, db=Depends(get_db)):
    trade_calc_sql_path = Path.cwd() / "sql" / "player_values" / "calc" / f"{rank_type}" / f"{platform}.sql"

    async with aiofiles.open(trade_calc_sql_path, mode='r') as trade_calc_file:
        tarde_calc_sql = await trade_calc_file.read()
    
    # Execute the query asynchronously
    result = await db.fetch(tarde_calc_sql)
    return result


@app.get("/league_summary")
async def league_summary(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db=Depends(get_db)):
    session_id = guid
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform in ['espn', 'cbs', 'nfl']:
        rank_source = 'contender'
    else:
        rank_source = 'power'
    
    if platform == 'dd':
        league_pos_col = (
            "sf_position_rank"
            if roster_type == "sf_value"
            else "position_rank"
        )
        league_type = (
            "sf_trade_value"
            if roster_type == "sf_value"
            else "trade_value"
        )

    if platform == 'sf':
        league_pos_col = (
            "superflex_sf_pos_rank"
            if roster_type == "sf_value"
            else "superflex_one_qb_pos_rank"
        )
        league_type = (
            "superflex_sf_value"
            if roster_type == "sf_value"
            else "superflex_one_qb_value"
        )

    elif platform == 'fc':
        league_pos_col = (
            "sf_position_rank" if league_type == "sf_value" else "one_qb_position_rank"
        )
    else:
        league_pos_col = ''

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "summary" / rank_source / f"{platform}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        power_summary_sql = await file.read()

        power_summary_sql = (power_summary_sql .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("league_pos_col", f"{league_pos_col}")
            .replace("'rank_type'", f"'{rank_type}'"))
    # Execute the query asynchronously and fetch results
    results = await db.fetch(power_summary_sql)
    return results


@app.get("/league_detail")
async def league_detail(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db=Depends(get_db)):
    session_id = guid
    league_type = 'sf_value' if roster_type.lower() == 'superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':
        league_pos_col = "superflex_sf_pos_rank" if roster_type.lower() == "superflex" else "superflex_one_qb_pos_rank"
        league_type = "superflex_sf_value" if roster_type.lower() == "superflex" else "superflex_one_qb_value"
    elif platform == 'dd':
        league_pos_col = "sf_position_rank" if roster_type.lower() == "superflex" else "position_rank"
        league_type = "sf_trade_value" if roster_type.lower() == "superflex" else "trade_value"
    elif platform == 'fc':
        league_pos_col = "sf_position_rank" if league_type == "sf_value" else "one_qb_position_rank"
    else:
        league_pos_col = ''

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "details" / "power" / f"{platform}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        power_detail_sql = await file.read()
        power_detail_sql = power_detail_sql.replace("'session_id'", f"'{session_id}'")
        power_detail_sql = power_detail_sql.replace("'league_id'", f"'{league_id}'")
        power_detail_sql = power_detail_sql.replace("league_type", f"{league_type}")
        power_detail_sql = power_detail_sql.replace("league_pos_col", f"{league_pos_col}")
        power_detail_sql = power_detail_sql.replace("'rank_type'", f"'{rank_type}'")

    # Execute the query asynchronously and fetch results
    results = await db.fetch(power_detail_sql)
    return results


@app.get("/trades_detail")
async def trades_detail(league_id: str, platform: str, roster_type: str, league_year: str, rank_type: str, db=Depends(get_db)):
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':
        league_type = "superflex_sf_value" if roster_type == "sf_value" else "superflex_one_qb_value"
    elif platform == 'dd':
        league_type = "sf_trade_value" if roster_type == "sf_value" else "trade_value"

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "details" / "trades" / f"{platform}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        trades_sql = await file.read()
        trades_sql = trades_sql.replace("'current_year'", f"'{league_year}'")
        trades_sql = trades_sql.replace("'league_id'", f"'{league_id}'")
        trades_sql = trades_sql.replace("league_type", f"{league_type}")
        trades_sql = trades_sql.replace("'rank_type'", f"'{rank_type}'")

    # Execute the query asynchronously and fetch results
    trades = await db.fetch(trades_sql)

    transaction_ids = list(set([(i["transaction_id"], i["status_updated"]) for i in trades]))
    transaction_ids.sort(key=lambda x: datetime.fromtimestamp(int(str(x[1])[:10])), reverse=True)

    managers_list = set([(i["display_name"], i["transaction_id"]) for i in trades])
    trades_dict = {
        transaction_id[0]: {
            manager[0]: [p for p in trades if p["display_name"] == manager[0] and p["transaction_id"] == transaction_id[0]]
            for manager in managers_list if manager[1] == transaction_id[0]
        } for transaction_id in transaction_ids
    }

    return trades_dict


@app.get("/trades_summary")
async def trades_summary(league_id: str, platform: str, roster_type: str, league_year: str, rank_type: str, db=Depends(get_db)):
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':
        league_type = "superflex_sf_value" if roster_type == "sf_value" else "superflex_one_qb_value"
    elif platform == 'dd':
        league_type = "sf_trade_value" if roster_type == "sf_value" else "trade_value"

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "summary" / "trades" / f"{platform}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        trades_sql = await file.read()
        trades_sql = trades_sql.replace("'current_year'", f"'{league_year}'")
        trades_sql = trades_sql.replace("'league_id'", f"'{league_id}'")
        trades_sql = trades_sql.replace("league_type", f"{league_type}")
        trades_sql = trades_sql.replace("'rank_type'", f"'{rank_type}'")

    # Execute the query asynchronously and fetch results
    db_resp_obj = await db.fetch(trades_sql)
    return db_resp_obj


@app.get("/contender_league_summary")
async def contender_league_summary(league_id: str, projection_source: str, guid: str, db=Depends(get_db)):
    print(league_id, projection_source)

    session_id = guid

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "summary" / "contender" / f"{projection_source}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        projections_sql = await file.read()
        projections_sql = projections_sql.replace("'session_id'", f"'{session_id}'")
        projections_sql = projections_sql.replace("'league_id'", f"'{league_id}'")

    # Execute the query asynchronously and fetch results
    db_resp_obj = await db.fetch(projections_sql)
    return db_resp_obj


@app.get("/contender_league_detail")
async def contender_league_detail(league_id: str, projection_source: str, guid: str, db=Depends(get_db)):
    print(league_id, projection_source)

    session_id = guid

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "details" / "contender" / f"{projection_source}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        projections_sql = await file.read()
        projections_sql = projections_sql.replace("'session_id'", f"'{session_id}'")
        projections_sql = projections_sql.replace("'league_id'", f"'{league_id}'")

    # Execute the query asynchronously and fetch results
    db_resp_obj = await db.fetch(projections_sql)
    return db_resp_obj


@app.get("/best_available")
async def best_available(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db=Depends(get_db)):
    session_id = guid
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':
        league_type = "superflex_sf_value" if roster_type == "sf_value" else "superflex_one_qb_value"
    elif platform == 'dd':
        league_type = "sf_trade_value" if roster_type == "sf_value" else "trade_value"
    else:
        league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'

    # Assemble SQL file path
    sql_file_path = Path.cwd() / "sql" / "best_available" / "power" / f"{platform}.sql"
    if not sql_file_path.exists():
        raise HTTPException(status_code=404, detail="SQL file not found")

    # Read and personalize the SQL query asynchronously
    async with aiofiles.open(sql_file_path, mode='r') as file:
        ba_sql = await file.read()
        ba_sql = (ba_sql.replace("'session_id'", f"'{session_id}'")
                    .replace("'league_id'", f"'{league_id}'")
                    .replace("league_type", f"{league_type}")
                    .replace("'rank_type'", f"'{rank_type}'")
                  )

    # Execute the query asynchronously and fetch results
    db_resp_obj = await db.fetch(ba_sql)
    return db_resp_obj



# class Player(BaseModel):
#     player_full_name: str
#     position: str
#     superflex_sf_value: float
#     superflex_sf_rank: int
#     superflex_sf_pos_rank: int
#     superflex_one_qb_value: float
#     superflex_one_qb_rank: int
#     superflex_one_qb_pos_rank: int
#     insert_date: Optional[str] = None

@app.get("/v1/rankings")
async def navigator_ranks_api(rank_type: str,  db=Depends(get_db)):
    rank_type = rank_type.lower()
    if rank_type not in ['dynasty', 'redraft']:
        raise HTTPException(status_code=400, detail="Invalid rank type")
    external_rankings_query = f"""
        SELECT player_full_name, _position, team, rank_type, superflex_sf_value, 
               superflex_sf_rank, superflex_sf_pos_rank, superflex_one_qb_value, 
               superflex_one_qb_rank, superflex_one_qb_pos_rank, insert_date
        FROM dynastr.sf_player_ranks 
        WHERE rank_type = '{rank_type}'
        ORDER BY superflex_sf_value DESC
    """
    try:
        result = await db.fetch(external_rankings_query)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.on_event("shutdown")
async def shutdown_event():
    await close_db()