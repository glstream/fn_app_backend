"""
MFL-specific API endpoints for direct league access.
These endpoints are used when users provide MFL league ID directly.
"""

from fastapi import Depends, HTTPException
from db import get_db
from superflex_models import LeagueDataModel
from mfl.mfl_routes import insert_current_leagues_mfl
from mfl.mfl_utils import insert_mfl_league_rosters
import asyncio


async def handle_mfl_league_direct(league_data: LeagueDataModel, db):
    """
    Handle direct MFL league access when user provides league ID.
    This combines league insertion and roster loading.
    
    Args:
        league_data: League data with league_id, session_id, and year
        db: Database connection
        
    Returns:
        Combined response with league and roster data
    """
    try:
        # Step 1: Insert league into current_leagues
        league_result = await insert_current_leagues_mfl(db, league_data)
        
        # Step 2: Load rosters for all teams
        league_id = league_data.league_id
        session_id = league_data.guid
        year = league_data.league_year
        
        # Get all franchise IDs from the league
        franchise_query = """
            SELECT DISTINCT user_id as franchise_id
            FROM dynastr.managers
            WHERE league_id = $1 AND platform = 'mfl'
        """
        franchises = await db.fetch(franchise_query, league_id)
        
        # Load rosters for each franchise
        roster_tasks = []
        for franchise in franchises:
            franchise_id = franchise['franchise_id']
            roster_tasks.append(
                insert_mfl_league_rosters(db, session_id, franchise_id, league_id, year)
            )
        
        if roster_tasks:
            roster_results = await asyncio.gather(*roster_tasks, return_exceptions=True)
            
            # Count successful roster insertions
            successful_rosters = sum(
                1 for result in roster_results 
                if not isinstance(result, Exception)
            )
        else:
            successful_rosters = 0
        
        return {
            "status": "success",
            "league": league_result,
            "rosters_loaded": successful_rosters,
            "total_franchises": len(franchises),
            "platform": "mfl",
            "league_id": league_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process MFL league: {e}")