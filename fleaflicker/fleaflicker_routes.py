"""
Fleaflicker-specific API routes.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, List, Dict
from pathlib import Path
import aiofiles
from datetime import datetime

from db import get_db
from superflex_models import UserDataModel, LeagueDataModel, RosterDataModel
from .fleaflicker_utils import (
    get_fleaflicker_user_id,
    get_fleaflicker_managers,
    insert_fleaflicker_teams,
    insert_fleaflicker_scoreboards,
    insert_fleaflicker_transactions,
    insert_fleaflicker_league_rosters
)
from .fleaflicker_client import fleaflicker_client
from fastapi_cache.decorator import cache
from utils import CACHE_EXPIRATION, LEAGUE_CACHE_EXPIRATION, SHORT_CACHE_EXPIRATION

router = APIRouter(prefix="/fleaflicker", tags=["fleaflicker"])


async def insert_current_leagues_fleaflicker(db, user_data: UserDataModel):
    """
    Insert Fleaflicker leagues into current_leagues table.
    
    Args:
        db: Database connection
        user_data: User data model with league year and username
    """
    user_name = user_data.user_name
    league_year = user_data.league_year
    session_id = user_data.guid
    
    print(f"DEBUG insert_current_leagues_fleaflicker: Starting for user {user_name}, year {league_year}, session {session_id}")
    
    # Get timestamp for cache busting if provided
    timestamp = getattr(user_data, 'timestamp', None)
    
    # Check if user_name is email or numeric ID
    if '@' in user_name:
        # Email-based lookup
        print(f"DEBUG: Email-based lookup for {user_name}")
        from .fleaflicker_utils import get_fleaflicker_user_leagues_by_email
        user_id, leagues = await get_fleaflicker_user_leagues_by_email(user_name, league_year, timestamp=timestamp)
        print(f"DEBUG: Email lookup returned - user_id: {user_id}, leagues count: {len(leagues) if leagues else 0}")
        
        # If we have leagues but no user_id, use the email as the user_id temporarily
        # We'll get the actual user_id from the standings API later
        if leagues and not user_id:
            print(f"DEBUG: Found leagues but no user_id, using email as temporary ID")
            user_id = user_name  # Use email as a temporary identifier
        
        if not leagues:
            print(f"DEBUG: No leagues found for email {user_name}")
            return {"status": "success", "leagues_inserted": 0, "message": "No leagues found for this email address"}
    else:
        # Numeric ID lookup (existing behavior)
        print(f"DEBUG: Numeric ID lookup for {user_name}")
        user_id = await get_fleaflicker_user_id(user_name)
        from .fleaflicker_utils import get_fleaflicker_user_leagues
        leagues = await get_fleaflicker_user_leagues(user_name, league_year, timestamp=timestamp)
        print(f"DEBUG: Numeric lookup returned - user_id: {user_id}, leagues count: {len(leagues) if leagues else 0}")
        if not leagues:
            print(f"DEBUG: No leagues found for username {user_name}")
            return {"status": "success", "leagues_inserted": 0, "message": "No leagues found for this username"}
    
    entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    
    # For each league, get the actual Fleaflicker numeric user ID from team standings
    # This ensures consistency with the ranks_summary table
    enriched_leagues = []
    for league in leagues:
        try:
            league_id = league[1]  # league_id is at index 1
            
            # Get standings to find the actual Fleaflicker user ID
            standings = await fleaflicker_client.fetch_league_standings(league_id)
            fleaflicker_user_id = None
            
            # Search through divisions and teams to find the user
            for division in standings.get("divisions", []):
                for team in division.get("teams", []):
                    for owner in team.get("owners", []):
                        # Match by email or display name (but prefer ID)
                        owner_email = owner.get("email", "")
                        owner_display = owner.get("displayName", "")
                        owner_id = owner.get("id")
                        
                        # If user_name is an email, match by email; otherwise match by display name
                        if '@' in user_name and owner_email:
                            if owner_email.lower() == user_name.lower():
                                fleaflicker_user_id = str(owner_id)
                                # print removed for production - DEBUG: Found user by email match - ID: {fleaflicker_user_id}")
                                break
                        elif owner_display == user_name:
                            fleaflicker_user_id = str(owner_id)
                            # print removed for production - DEBUG: Found user by display name match - ID: {fleaflicker_user_id}")
                            break
                    if fleaflicker_user_id:
                        break
                if fleaflicker_user_id:
                    break
            
            # If we found the Fleaflicker user ID, use it; otherwise fallback to original user_id
            actual_user_id = fleaflicker_user_id if fleaflicker_user_id else user_id
            print(f"DEBUG: League {league_id} - fleaflicker_user_id: {fleaflicker_user_id}, original user_id: {user_id}, actual_user_id: {actual_user_id}")
            
            # Create enriched league tuple with the correct user ID
            enriched_league = list(league)  # Convert to list for modification
            enriched_leagues.append((actual_user_id, enriched_league))
            
        except Exception as e:
            print(f"Warning: Could not get Fleaflicker user ID for league {league[1]}: {e}")
            # Fallback to original user_id
            enriched_leagues.append((user_id, list(league)))
    
    # Clean existing Fleaflicker leagues for this session
    # Use a broader delete since we might have different user_ids now
    delete_query = """
        DELETE FROM dynastr.current_leagues 
        WHERE session_id = $1 AND platform = 'fleaflicker'
        AND league_id = ANY($2::text[])
    """
    league_ids = [league[1][1] for league in enriched_leagues]  # Get league IDs
    
    try:
        async with db.transaction():
            await db.execute(delete_query, session_id, league_ids)
            
            # Prepare league data for insertion with correct user IDs
            values = [
                (
                    session_id,
                    actual_user_id,  # Use the Fleaflicker numeric user ID
                    user_name,
                    league[1],  # league_id
                    league[0],  # league_name
                    league[2],  # avatar
                    league[3],  # total_rosters
                    league[4],  # qb_cnt
                    league[5],  # rb_cnt
                    league[6],  # wr_cnt
                    league[7],  # te_cnt
                    league[8],  # flex_cnt
                    league[9],  # sf_cnt
                    league[10], # starter_cnt
                    league[11], # total_roster_cnt
                    league[12], # sport
                    entry_time,
                    league[13], # rf_cnt
                    league[14], # league_cat
                    league[15], # league_year
                    league[16], # previous_league_id
                    "active",   # league_status
                    "fleaflicker"  # platform
                )
                for actual_user_id, league in enriched_leagues
            ]
            
            # Insert with platform column
            await db.executemany("""
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
            """, values)
            
            # Save to user_searches table for future reference
            # Use the first found Fleaflicker user ID if available, and store username not email
            search_user_id = enriched_leagues[0][0] if enriched_leagues else user_id
            
            # If user_name is an email, we need to extract the username from the API response
            display_name = user_name
            user_identifier = user_name
            
            # For email-based lookup, let's use the username from the standings
            if '@' in user_name and enriched_leagues:
                try:
                    # Get the first league and fetch its standings to find the username
                    first_league_id = enriched_leagues[0][1][1]  # league_id is at index 1
                    standings = await fleaflicker_client.fetch_league_standings(first_league_id)
                    
                    for division in standings.get("divisions", []):
                        for team in division.get("teams", []):
                            for owner in team.get("owners", []):
                                if owner.get("email", "").lower() == user_name.lower():
                                    display_name = owner.get("displayName", user_name)
                                    user_identifier = display_name  # Use username, not email
                                    # print removed for production - DEBUG: Using username '{display_name}' instead of email for storage")
                                    break
                except Exception as e:
                    # print removed for production - DEBUG: Could not extract username, using original identifier: {e}")
                    pass
            
            await db.execute("""
                INSERT INTO dynastr.user_searches (
                    user_identifier, 
                    platform, 
                    display_name,
                    user_id,
                    league_year,
                    last_used
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (user_identifier, platform) 
                DO UPDATE SET 
                    last_used = NOW(),
                    league_year = EXCLUDED.league_year,
                    user_id = EXCLUDED.user_id
            """, user_identifier, 'fleaflicker', display_name, search_user_id, league_year)
            
        print(f"DEBUG: Successfully inserted {len(values)} Fleaflicker leagues for session {session_id}")
        return {"status": "success", "leagues_inserted": len(values)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert Fleaflicker leagues: {e}")


@router.get("/saved-usernames")
async def get_saved_usernames(db=Depends(get_db)):
    """
    Get saved Fleaflicker usernames from previous searches.
    
    Returns:
        List of previously searched usernames with metadata
    """
    try:
        query = """
            SELECT 
                user_identifier,
                display_name,
                user_id,
                league_year,
                last_used
            FROM dynastr.user_searches
            WHERE platform = 'fleaflicker'
            ORDER BY last_used DESC
            LIMIT 20
        """
        
        results = await db.fetch(query)
        
        usernames = []
        for row in results:
            usernames.append({
                "user_identifier": row["user_identifier"],
                "display_name": row["display_name"],
                "user_id": row["user_id"],
                "league_year": row["league_year"],
                "last_used": row["last_used"].isoformat() if row["last_used"] else None
            })
        
        return {"status": "success", "usernames": usernames}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch saved usernames: {e}")


@router.get("/league/{league_id}/standings")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_fleaflicker_standings(league_id: str, season: Optional[str] = None):
    """
    Get standings for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        season: Optional season year
        
    Returns:
        League standings data
    """
    try:
        standings = await fleaflicker_client.fetch_league_standings(league_id, season)
        return standings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch standings: {e}")


@router.get("/league/{league_id}/scoreboard")
@cache(expire=SHORT_CACHE_EXPIRATION)
async def get_fleaflicker_scoreboard(
    league_id: str,
    season: Optional[str] = None,
    scoring_period: Optional[str] = None
):
    """
    Get scoreboard for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        season: Optional season year
        scoring_period: Optional week number
        
    Returns:
        League scoreboard data
    """
    try:
        scoreboard = await fleaflicker_client.fetch_league_scoreboard(
            league_id, season, scoring_period
        )
        return scoreboard
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch scoreboard: {e}")


@router.get("/league/{league_id}/boxscore/{game_id}")
@cache(expire=SHORT_CACHE_EXPIRATION)
async def get_fleaflicker_boxscore(
    league_id: str,
    game_id: str,
    scoring_period: Optional[str] = None
):
    """
    Get detailed boxscore for a specific game.
    
    Args:
        league_id: Fleaflicker league ID
        game_id: Fantasy game ID
        scoring_period: Optional week number
        
    Returns:
        Detailed boxscore data
    """
    try:
        boxscore = await fleaflicker_client.fetch_league_boxscore(
            league_id, game_id, scoring_period
        )
        return boxscore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch boxscore: {e}")


@router.get("/league/{league_id}/transactions")
@cache(expire=SHORT_CACHE_EXPIRATION)
async def get_fleaflicker_transactions(
    league_id: str,
    team_id: Optional[str] = None,
    offset: int = 0
):
    """
    Get transaction history for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        team_id: Optional team ID to filter
        offset: Pagination offset
        
    Returns:
        Transaction history
    """
    try:
        transactions = await fleaflicker_client.fetch_league_transactions(
            league_id, team_id, offset
        )
        return transactions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch transactions: {e}")


@router.get("/league/{league_id}/rosters")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_fleaflicker_rosters(
    league_id: str,
    season: Optional[str] = None,
    scoring_period: Optional[str] = None
):
    """
    Get all rosters for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        season: Optional season year
        scoring_period: Optional week number
        
    Returns:
        All team rosters
    """
    try:
        rosters = await fleaflicker_client.fetch_league_rosters(
            league_id, season, scoring_period
        )
        return rosters
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch rosters: {e}")


@router.get("/team/{league_id}/{team_id}/roster")
@cache(expire=LEAGUE_CACHE_EXPIRATION)
async def get_fleaflicker_team_roster(
    league_id: str,
    team_id: str,
    season: Optional[str] = None,
    scoring_period: Optional[str] = None
):
    """
    Get roster for a specific team.
    
    Args:
        league_id: Fleaflicker league ID
        team_id: Team ID
        season: Optional season year
        scoring_period: Optional week number
        
    Returns:
        Team roster details
    """
    try:
        roster = await fleaflicker_client.fetch_roster(
            league_id, team_id, season, scoring_period
        )
        return roster
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch team roster: {e}")


@router.get("/players")
@cache(expire=CACHE_EXPIRATION)
async def get_fleaflicker_players(
    position: Optional[str] = None,
    team: Optional[str] = None,
    owned: Optional[bool] = None,
    offset: int = 0
):
    """
    Get player listing with filters.
    
    Args:
        position: Position filter (QB, RB, WR, TE)
        team: NFL team filter
        owned: Ownership filter
        offset: Pagination offset
        
    Returns:
        Filtered player listing
    """
    try:
        players = await fleaflicker_client.fetch_player_listing(
            filter_position=position,
            filter_team=team,
            filter_owned=owned,
            result_offset=offset
        )
        return players
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch players: {e}")


@router.post("/sync_league/{league_id}")
async def sync_fleaflicker_league(
    league_id: str,
    session_id: str,
    user_id: str,
    db=Depends(get_db)
):
    """
    Sync all data for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        session_id: Session ID
        user_id: User ID
        db: Database connection
        
    Returns:
        Sync status
    """
    try:
        # Get current season
        season = str(datetime.now().year)
        
        # Insert/update teams
        await insert_fleaflicker_teams(db, session_id, league_id)
        
        # Insert/update rosters
        await insert_fleaflicker_league_rosters(db, session_id, user_id, league_id)
        
        # Insert current week scoreboard
        # TODO: Determine current NFL week dynamically
        current_week = 1
        await insert_fleaflicker_scoreboards(db, session_id, league_id, season, current_week)
        
        # Insert transactions
        await insert_fleaflicker_transactions(db, session_id, league_id)
        
        return {
            "status": "success",
            "message": f"Successfully synced Fleaflicker league {league_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to sync league: {e}")


@router.get("/league/{league_id}/draft")
@cache(expire=CACHE_EXPIRATION)
async def get_fleaflicker_draft(
    league_id: str,
    season: Optional[str] = None,
    draft_number: int = 1
):
    """
    Get draft board for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        season: Optional season year
        draft_number: Draft number (for multiple drafts)
        
    Returns:
        Draft board data
    """
    try:
        draft = await fleaflicker_client.fetch_league_draft_board(
            league_id, season, draft_number
        )
        return draft
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch draft: {e}")