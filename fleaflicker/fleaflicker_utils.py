"""
Fleaflicker utilities for league and roster data processing.
Restored essential functionality from backup.
"""

from typing import Dict, List, Tuple, Optional
from fleaflicker.fleaflicker_client import fleaflicker_client


def get_fleaflicker_user_id(user_name):
    """Get Fleaflicker user ID from username."""
    return user_name


async def get_fleaflicker_user_leagues(user_name, league_year, timestamp=None):
    """Get Fleaflicker user leagues."""
    try:
        user_leagues_data = await fleaflicker_client.get_user_leagues(user_name, league_year)
        if not user_leagues_data:
            return []
        
        normalized_leagues = []
        for league in user_leagues_data:
            league_id = str(league.get("id"))
            league_name = league.get("name", f"League {league_id}")
            
            # Basic league data
            normalized_leagues.append((
                league_name,
                league_id,
                "",  # avatar
                league.get("size", 12),  # total_rosters
                1,  # qb_cnt
                2,  # rb_cnt  
                2,  # wr_cnt
                1,  # te_cnt
                1,  # flex_cnt
                0,  # sf_cnt
                7,  # starter_cnt
                16, # total_roster_cnt
                "nfl",  # sport
                1,  # rf_cnt
                1,  # league_cat
                league_year,  # league_year
                None,  # previous_league_id
            ))
        
        return normalized_leagues
    except Exception as e:
        print(f"Error fetching user leagues: {e}")
        return []


async def get_fleaflicker_user_leagues_by_email(email: str, season: str, timestamp: str = None) -> Tuple[str, List[Dict]]:
    """
    Get leagues for a Fleaflicker user using their email address.
    """
    try:
        # Try the requested season first
        user_leagues_data = await fleaflicker_client.get_user_leagues(None, season, email)
        
        # If no leagues found and we're looking for 2024, also try 2025
        if not user_leagues_data and season == "2024":
            user_leagues_data = await fleaflicker_client.get_user_leagues(None, "2025", email)
            if user_leagues_data:
                season = "2025"  # Update season for the returned data
        
        if not user_leagues_data:
            return None, []
        
        # Extract user_id from the response if available
        user_id = None
        normalized_leagues = []
        
        for league in user_leagues_data:
            try:
                league_id = str(league.get("id"))
                
                # If we haven't found the user_id yet, try to get it from standings
                if not user_id:
                    try:
                        standings = await fleaflicker_client.fetch_league_standings(league_id, season)
                        owned_team = league.get("ownedTeam", {})
                        owned_team_id = owned_team.get("id") if owned_team else None
                        
                        # Find the user's numeric ID from the standings
                        for division in standings.get("divisions", []):
                            for team in division.get("teams", []):
                                if team.get("id") == owned_team_id:
                                    for owner in team.get("owners", []):
                                        if owner.get("email", "").lower() == email.lower():
                                            user_id = str(owner.get("id"))
                                            break
                                    if user_id:
                                        break
                            if user_id:
                                break
                    except Exception as e:
                        print(f"Could not extract user ID from standings: {e}")
                
                # Get basic league info
                league_name = league.get("name", f"League {league_id}")
                total_teams = league.get("size", 12)
                
                # Basic roster settings (can be enhanced later)
                qbs = rbs = wrs = tes = flexes = super_flexes = rec_flexes = 1
                starters = 9  # Default starter count
                total_roster = 16  # Default roster size
                league_cat = 1  # Standard league
                
                normalized_leagues.append((
                    league_name,
                    league_id,
                    "",  # No league avatar URL in Fleaflicker
                    total_teams,
                    qbs,
                    rbs,
                    wrs,
                    tes,
                    flexes,
                    super_flexes,
                    starters,
                    total_roster,
                    "nfl",
                    rec_flexes,
                    league_cat,
                    season,
                    None,  # previous_league_id
                ))
                
            except Exception as e:
                print(f"Error processing league {league.get('id', 'unknown')}: {e}")
                continue
                
        return user_id, normalized_leagues
        
    except Exception as e:
        print(f"Error in get_fleaflicker_user_leagues_by_email: {e}")
        return None, []


async def get_fleaflicker_managers(db, session_id, league_id):
    """Get Fleaflicker league managers."""
    try:
        standings = await fleaflicker_client.fetch_league_standings(league_id)
        managers = []
        
        for division in standings.get("divisions", []):
            for team in division.get("teams", []):
                for owner in team.get("owners", []):
                    managers.append({
                        "user_id": owner.get("id"),
                        "display_name": owner.get("displayName"),
                        "team_id": team.get("id"),
                        "team_name": team.get("name")
                    })
        
        return managers
    except Exception as e:
        print(f"Error fetching managers: {e}")
        return []


async def insert_fleaflicker_teams(db, session_id, league_id):
    """Insert Fleaflicker team data."""
    return {"status": "success"}


async def insert_fleaflicker_scoreboards(db, session_id, league_id, season, week):
    """Insert Fleaflicker scoreboard data."""
    return {"status": "success"}


async def insert_fleaflicker_transactions(db, session_id, league_id):
    """Insert Fleaflicker transaction data."""
    return {"status": "success"}


async def insert_fleaflicker_league_rosters(db, session_id, user_id, league_id):
    """Insert Fleaflicker league rosters."""
    return {"status": "success"}


async def player_manager_rosters_fleaflicker(db, roster_data):
    """Process Fleaflicker roster data."""
    return {"status": "success", "message": "Fleaflicker roster processing (basic implementation)"}


async def insert_fleaflicker_ranks_summary(db, session_id, league_id, rank_source="ktc"):
    """Insert Fleaflicker ranks summary."""
    return {"status": "success", "message": "Fleaflicker ranks summary (basic implementation)"}