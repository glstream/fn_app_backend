"""
MyFantasyLeague-specific API routes.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, List, Dict
from datetime import datetime

from db import get_db
from superflex_models import UserDataModel, LeagueDataModel, RosterDataModel
from .mfl_utils import (
    get_mfl_league_by_id,
    insert_mfl_teams,
    insert_mfl_league_rosters,
    insert_mfl_transactions,
    insert_mfl_draft_picks,
    get_mfl_power_rankings
)
from .mfl_client import mfl_client
from fastapi_cache.decorator import cache
from utils import CACHE_EXPIRATION, LEAGUE_CACHE_EXPIRATION, SHORT_CACHE_EXPIRATION

router = APIRouter(prefix="/mfl", tags=["mfl"])


async def insert_current_leagues_mfl(db, league_data: LeagueDataModel):
    """
    Insert MFL league into current_leagues table.
    Unlike Sleeper/Fleaflicker which use username, MFL uses direct league ID.
    
    Args:
        db: Database connection
        league_data: League data model with league ID and year
    """
    league_id = league_data.league_id
    league_year = league_data.league_year or str(datetime.now().year)
    session_id = league_data.guid
    
    try:
        # Get league info from MFL
        normalized_league, managers = await get_mfl_league_by_id(league_id, league_year)
        
        entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        
        # For MFL, we'll use the first franchise as the "user"
        # since we're going directly to league view
        user_id = managers[0]["franchise_id"] if managers else league_id
        user_name = managers[0]["owner_name"] if managers else "MFL User"
        
        # Clean existing MFL league for this session
        delete_query = """
            DELETE FROM dynastr.current_leagues 
            WHERE session_id = $1 AND league_id = $2 AND platform = 'mfl'
        """
        
        async with db.transaction():
            await db.execute(delete_query, session_id, league_id)
            
            # Insert league data
            await db.execute("""
                INSERT INTO dynastr.current_leagues (
                    session_id, user_id, user_name, league_id, league_name, avatar,
                    total_rosters, qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt,
                    starter_cnt, total_roster_cnt, sport, insert_date, rf_cnt, league_cat,
                    league_year, previous_league_id, league_status, platform
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                ON CONFLICT (session_id, league_id) DO UPDATE 
                SET
                    user_id = excluded.user_id,
                    user_name = excluded.user_name,
                    league_name = excluded.league_name,
                    avatar = excluded.avatar,
                    total_rosters = excluded.total_rosters,
                    qb_cnt = excluded.qb_cnt,
                    rb_cnt = excluded.rb_cnt,
                    wr_cnt = excluded.wr_cnt,
                    te_cnt = excluded.te_cnt,
                    flex_cnt = excluded.flex_cnt,
                    sf_cnt = excluded.sf_cnt,
                    starter_cnt = excluded.starter_cnt,
                    total_roster_cnt = excluded.total_roster_cnt,
                    sport = excluded.sport,
                    insert_date = excluded.insert_date,
                    rf_cnt = excluded.rf_cnt,
                    league_cat = excluded.league_cat,
                    league_year = excluded.league_year,
                    previous_league_id = excluded.previous_league_id,
                    league_status = excluded.league_status,
                    platform = excluded.platform
            """, 
                session_id,
                user_id,
                user_name,
                normalized_league["league_id"],
                normalized_league["league_name"],
                "",  # avatar
                normalized_league["total_rosters"],
                normalized_league["qb_cnt"],
                normalized_league["rb_cnt"],
                normalized_league["wr_cnt"],
                normalized_league["te_cnt"],
                normalized_league["flex_cnt"],
                normalized_league["sf_cnt"],
                normalized_league["starter_cnt"],
                normalized_league["total_roster_cnt"],
                normalized_league["sport"],
                entry_time,
                normalized_league["rf_cnt"],
                normalized_league["league_cat"],
                normalized_league["league_year"],
                None,  # previous_league_id
                "active",
                "mfl"
            )
            
            # Insert managers
            await insert_mfl_teams(db, session_id, league_id, league_year)
            
        return {
            "status": "success", 
            "league_inserted": normalized_league["league_name"],
            "league_id": league_id,
            "managers": len(managers)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert MFL league: {e}")


@router.get("/league/{league_id}")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_league_info(league_id: str, year: Optional[str] = None):
    """
    Get comprehensive league information for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        
    Returns:
        League information including settings and teams
    """
    try:
        league_info = await mfl_client.get_league_info(league_id, year)
        return league_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch league info: {e}")


@router.get("/league/{league_id}/rosters")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_rosters(
    league_id: str,
    year: Optional[str] = None,
    week: Optional[str] = None
):
    """
    Get all rosters for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        week: Optional week number
        
    Returns:
        All team rosters
    """
    try:
        rosters = await mfl_client.get_rosters(league_id, year, week)
        return rosters
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch rosters: {e}")


@router.get("/league/{league_id}/standings")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_standings(league_id: str, year: Optional[str] = None):
    """
    Get standings for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        
    Returns:
        League standings data
    """
    try:
        standings = await mfl_client.get_standings(league_id, year)
        return standings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch standings: {e}")


@router.get("/league/{league_id}/transactions")
@cache(expire=SHORT_CACHE_EXPIRATION)
async def get_mfl_transactions(
    league_id: str,
    year: Optional[str] = None,
    trans_type: Optional[str] = None,
    days: Optional[int] = 30
):
    """
    Get transaction history for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        trans_type: Transaction type filter
        days: Number of days to look back
        
    Returns:
        Transaction history
    """
    try:
        transactions = await mfl_client.get_transactions(
            league_id, year, trans_type, days
        )
        return transactions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch transactions: {e}")


@router.get("/league/{league_id}/draft")
@cache(expire=CACHE_EXPIRATION)
async def get_mfl_draft(league_id: str, year: Optional[str] = None):
    """
    Get draft results for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        
    Returns:
        Draft results data
    """
    try:
        draft = await mfl_client.get_draft_results(league_id, year)
        return draft
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch draft results: {e}")


@router.get("/league/{league_id}/picks")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_draft_picks(league_id: str, year: Optional[str] = None):
    """
    Get future draft picks for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        
    Returns:
        Future draft picks data
    """
    try:
        picks = await mfl_client.get_future_draft_picks(league_id, year)
        return picks
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch draft picks: {e}")


@router.get("/league/{league_id}/scoring")
@cache(expire=SHORT_CACHE_EXPIRATION)
async def get_mfl_scoring(
    league_id: str,
    year: Optional[str] = None,
    week: Optional[str] = None
):
    """
    Get scoring data for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        week: Optional week number
        
    Returns:
        Scoring data
    """
    try:
        scoring = await mfl_client.get_scoring(league_id, year, week)
        return scoring
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch scoring: {e}")


@router.get("/league/{league_id}/schedule")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_schedule(
    league_id: str,
    year: Optional[str] = None,
    week: Optional[str] = None
):
    """
    Get matchup/schedule data for an MFL league.
    
    Args:
        league_id: MFL league ID
        year: Optional season year
        week: Optional week number
        
    Returns:
        Schedule data
    """
    try:
        schedule = await mfl_client.get_matchup(league_id, year, week)
        return schedule
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schedule: {e}")


@router.get("/players")
@cache(expire=CACHE_EXPIRATION)
async def get_mfl_players(
    year: Optional[str] = None,
    players: Optional[str] = None
):
    """
    Get player information.
    
    Args:
        year: Optional season year
        players: Comma-separated player IDs
        
    Returns:
        Player information
    """
    try:
        player_data = await mfl_client.get_players(year, players)
        return player_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch players: {e}")


@router.post("/sync_league/{league_id}")
async def sync_mfl_league(
    league_id: str,
    session_id: str,
    user_id: str,
    year: Optional[str] = None,
    db=Depends(get_db)
):
    """
    Sync all data for an MFL league.
    
    Args:
        league_id: MFL league ID
        session_id: Session ID
        user_id: User ID (franchise ID for MFL)
        year: Optional season year
        db: Database connection
        
    Returns:
        Sync status
    """
    try:
        # Insert/update teams
        teams_count = await insert_mfl_teams(db, session_id, league_id, year)
        
        # Insert/update rosters
        rosters_count = await insert_mfl_league_rosters(db, session_id, user_id, league_id, year)
        
        # Insert transactions (optional)
        trans_count = await insert_mfl_transactions(db, session_id, league_id, year)
        
        # Insert draft picks (optional)
        picks_count = await insert_mfl_draft_picks(db, session_id, league_id, year)
        
        return {
            "status": "success",
            "message": f"Successfully synced MFL league {league_id}",
            "teams": teams_count,
            "roster_entries": rosters_count,
            "transactions": trans_count,
            "draft_picks": picks_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to sync league: {e}")


@router.get("/league/{league_id}/power_rankings")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_mfl_league_power_rankings(
    league_id: str,
    session_id: str,
    db=Depends(get_db)
):
    """
    Get power rankings for an MFL league.
    
    Args:
        league_id: MFL league ID
        session_id: Session ID
        db: Database connection
        
    Returns:
        Power rankings data
    """
    try:
        rankings = await get_mfl_power_rankings(db, session_id, league_id)
        return {"rankings": rankings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get power rankings: {e}")