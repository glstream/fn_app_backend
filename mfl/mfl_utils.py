"""
MyFantasyLeague utility functions for data processing and database operations.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import traceback

from .mfl_client import (
    mfl_client,
    normalize_mfl_league_data,
    normalize_mfl_roster_data,
    extract_mfl_managers,
    get_mfl_league_with_rosters
)


async def get_mfl_league_by_id(league_id: str, year: str = None) -> Tuple[Dict, List[Dict]]:
    """
    Get MFL league data by league ID.
    
    Args:
        league_id: MFL league ID
        year: Season year
        
    Returns:
        Tuple of (normalized_league_data, managers)
    """
    data = await get_mfl_league_with_rosters(league_id, year)
    
    # Return league info and managers for insertion
    return data["league"], data["managers"]


async def insert_mfl_teams(db, session_id: str, league_id: str, year: str = None):
    """
    Insert MFL team/franchise data into the database.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: MFL league ID
        year: Season year
    """
    try:
        # Get league data
        league_data = await mfl_client.get_league_info(league_id, year)
        
        # Extract franchise information
        franchises = league_data.get("franchises", {}).get("franchise", [])
        if not isinstance(franchises, list):
            franchises = [franchises]
        
        # Prepare data for insertion
        teams_data = []
        for franchise in franchises:
            owner = franchise.get("owner", {})
            if isinstance(owner, str):
                owner_name = owner
            else:
                owner_name = owner.get("name", "Unknown")
            
            teams_data.append((
                "mfl",
                franchise.get("id"),
                league_id,
                franchise.get("icon", ""),
                franchise.get("name", owner_name),
                franchise.get("division", "")
            ))
        
        # Clean existing teams
        async with db.transaction():
            await db.execute("""
                DELETE FROM dynastr.managers 
                WHERE league_id = $1 AND platform = 'mfl'
            """, league_id)
            
            # Insert new teams
            await db.executemany("""
                INSERT INTO dynastr.managers (platform, user_id, league_id, avatar, display_name, division)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (platform, user_id, league_id) DO UPDATE
                SET avatar = EXCLUDED.avatar,
                    display_name = EXCLUDED.display_name,
                    division = EXCLUDED.division
            """, teams_data)
            
        return len(teams_data)
        
    except Exception as e:
        print(f"Error inserting MFL teams: {e}")
        traceback.print_exc()
        raise


async def insert_mfl_league_rosters(db, session_id: str, user_id: str, league_id: str, year: str = None):
    """
    Insert MFL roster data into the database.
    
    Args:
        db: Database connection
        session_id: Session ID
        user_id: User ID (franchise ID for MFL)
        league_id: MFL league ID
        year: Season year
    """
    try:
        # Get roster data
        roster_data = await mfl_client.get_rosters(league_id, year)
        
        # Get player information for mapping
        all_player_ids = set()
        franchises = roster_data.get("franchise", [])
        if not isinstance(franchises, list):
            franchises = [franchises]
        
        for franchise in franchises:
            players = franchise.get("player", [])
            if not isinstance(players, list):
                players = [players] if players else []
            for player in players:
                if player.get("id"):
                    all_player_ids.add(player["id"])
        
        # Get player details
        player_details = {}
        if all_player_ids:
            players_str = ",".join(all_player_ids)
            player_info = await mfl_client.get_players(year, players_str)
            
            for player in player_info.get("player", []):
                player_details[player["id"]] = {
                    "name": player.get("name", ""),
                    "position": player.get("position", ""),
                    "team": player.get("team", "")
                }
        
        # Prepare roster data for insertion
        roster_entries = []
        for franchise in franchises:
            franchise_id = franchise.get("id")
            players = franchise.get("player", [])
            if not isinstance(players, list):
                players = [players] if players else []
            
            for player in players:
                player_id = player.get("id")
                if player_id and player_id in player_details:
                    roster_entries.append((
                        session_id,
                        franchise_id,
                        league_id,
                        player_id,
                        player_details[player_id]["name"],
                        player_details[player_id]["position"],
                        player_details[player_id]["team"],
                        player.get("status", "ROSTER"),
                        player.get("salary", 0)
                    ))
        
        # Clean existing rosters
        async with db.transaction():
            await db.execute("""
                DELETE FROM dynastr.league_players 
                WHERE session_id = $1 AND league_id = $2
            """, session_id, league_id)
            
            # Insert new rosters
            if roster_entries:
                await db.executemany("""
                    INSERT INTO dynastr.league_players 
                    (session_id, user_id, league_id, player_id, player_name, 
                     player_position, team, status, salary)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, roster_entries)
        
        return len(roster_entries)
        
    except Exception as e:
        print(f"Error inserting MFL rosters: {e}")
        traceback.print_exc()
        raise


async def insert_mfl_transactions(db, session_id: str, league_id: str, year: str = None, days: int = 30):
    """
    Insert MFL transaction data into the database.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: MFL league ID
        year: Season year
        days: Number of days of transactions to fetch
    """
    try:
        # Get transaction data
        trans_data = await mfl_client.get_transactions(league_id, year, days=days)
        
        transactions = trans_data.get("transaction", [])
        if not isinstance(transactions, list):
            transactions = [transactions] if transactions else []
        
        # Prepare transaction data
        trans_entries = []
        for trans in transactions:
            # Parse transaction details
            trans_type = trans.get("type", "")
            timestamp = trans.get("timestamp", "")
            
            # Convert timestamp to datetime
            if timestamp:
                trans_date = datetime.fromtimestamp(int(timestamp))
            else:
                trans_date = datetime.now()
            
            trans_entries.append((
                session_id,
                league_id,
                trans.get("franchise", ""),
                trans_type,
                trans.get("transaction", ""),  # Full transaction text
                trans_date,
                trans.get("franchise2", ""),  # For trades
                trans.get("player", "")  # Player ID if applicable
            ))
        
        # Insert transactions
        if trans_entries:
            async with db.transaction():
                # Note: You may need to create a transactions table for MFL
                # or adapt to existing transaction table structure
                await db.executemany("""
                    INSERT INTO dynastr.mfl_transactions 
                    (session_id, league_id, franchise_id, transaction_type, 
                     description, transaction_date, franchise2_id, player_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT DO NOTHING
                """, trans_entries)
        
        return len(trans_entries)
        
    except Exception as e:
        print(f"Error inserting MFL transactions: {e}")
        traceback.print_exc()
        # Don't raise - transactions are optional
        return 0


async def insert_mfl_draft_picks(db, session_id: str, league_id: str, year: str = None):
    """
    Insert MFL future draft picks into the database.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: MFL league ID
        year: Season year
    """
    try:
        # Get future draft picks
        picks_data = await mfl_client.get_future_draft_picks(league_id, year)
        
        picks = picks_data.get("futureDraftPick", [])
        if not isinstance(picks, list):
            picks = [picks] if picks else []
        
        # Prepare picks data
        picks_entries = []
        for pick in picks:
            picks_entries.append((
                league_id,
                session_id,
                pick.get("franchise", ""),  # Current owner
                pick.get("originalFranchise", ""),  # Original owner
                int(pick.get("year", 0)),
                int(pick.get("round", 0))
            ))
        
        # Clean existing picks
        async with db.transaction():
            await db.execute("""
                DELETE FROM dynastr.draft_picks 
                WHERE league_id = $1 AND session_id = $2
            """, league_id, session_id)
            
            # Insert new picks
            if picks_entries:
                await db.executemany("""
                    INSERT INTO dynastr.draft_picks 
                    (league_id, session_id, owner_id, original_owner_id, 
                     season, round)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, picks_entries)
        
        return len(picks_entries)
        
    except Exception as e:
        print(f"Error inserting MFL draft picks: {e}")
        traceback.print_exc()
        # Don't raise - draft picks are optional
        return 0


async def get_mfl_power_rankings(db, session_id: str, league_id: str):
    """
    Calculate power rankings for MFL league teams.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: MFL league ID
        
    Returns:
        Power rankings data
    """
    try:
        # Query to get team values using existing ranking system
        query = """
            WITH team_values AS (
                SELECT 
                    lp.user_id as franchise_id,
                    m.display_name,
                    SUM(COALESCE(spr.ktc_sf_value, 0)) as ktc_value,
                    SUM(COALESCE(spr.fc_sf_value, 0)) as fc_value,
                    SUM(COALESCE(spr.dp_sf_value, 0)) as dp_value,
                    SUM(COALESCE(spr.dd_sf_value, 0)) as dd_value
                FROM dynastr.league_players lp
                LEFT JOIN dynastr.managers m ON lp.user_id = m.user_id 
                    AND lp.league_id = m.league_id
                LEFT JOIN dynastr.sf_player_ranks spr ON lp.player_id = spr.ktc_player_id
                WHERE lp.session_id = $1 AND lp.league_id = $2
                GROUP BY lp.user_id, m.display_name
            )
            SELECT 
                franchise_id,
                display_name,
                ktc_value,
                fc_value,
                dp_value,
                dd_value,
                RANK() OVER (ORDER BY ktc_value DESC) as ktc_rank,
                RANK() OVER (ORDER BY fc_value DESC) as fc_rank,
                RANK() OVER (ORDER BY dp_value DESC) as dp_rank,
                RANK() OVER (ORDER BY dd_value DESC) as dd_rank
            FROM team_values
            ORDER BY ktc_value DESC
        """
        
        results = await db.fetch(query, session_id, league_id)
        
        return [dict(row) for row in results]
        
    except Exception as e:
        print(f"Error calculating MFL power rankings: {e}")
        traceback.print_exc()
        return []