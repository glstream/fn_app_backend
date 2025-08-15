"""
Fleaflicker-specific utility functions for data ingestion and processing.
Follows patterns from utils.py for Sleeper integration.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
# from fastapi_cache.decorator import cache  # Disabled temporarily
from .fleaflicker_client import fleaflicker_client
from utils import CACHE_EXPIRATION, LEAGUE_CACHE_EXPIRATION, SHORT_CACHE_EXPIRATION


# @cache(expire=CACHE_EXPIRATION)  # Disabled temporarily
async def get_fleaflicker_user_id(username: str) -> Optional[str]:
    """
    Get Fleaflicker user ID from username.
    
    Note: Fleaflicker doesn't have a direct user lookup endpoint.
    We store the username and let users provide league IDs directly.
    
    Args:
        username: Fleaflicker username/display name
        
    Returns:
        Username as identifier (we'll match against league rosters)
    """
    # Return the username itself as the identifier
    # We'll match this against display names in leagues
    return username


# @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily 
async def get_fleaflicker_user_leagues_by_ids(username: str, season: str, league_ids: List[str] = None, timestamp: str = None) -> List[Dict]:
    """
    Get leagues for a Fleaflicker user using provided league IDs.
    
    This is the working implementation that uses league IDs since FetchUserLeagues 
    requires numeric user_id that we may not have.
    
    Args:
        username: Fleaflicker username
        season: Season year  
        league_ids: List of known league IDs for this user
        timestamp: Optional timestamp for cache busting
        
    Returns:
        List of league dictionaries with normalized structure
    """
    if not league_ids:
        return []
    
    normalized_leagues = []
    
    for league_id in league_ids:
        try:
            # Fetch league standings to get basic info
            standings = await fleaflicker_client.fetch_league_standings(league_id)
            
            # Find if user is in this league (check all divisions)
            user_team = None
            total_teams = 0
            for division in standings.get("divisions", []):
                total_teams += len(division.get("teams", []))
                for team in division.get("teams", []):
                    for owner in team.get("owners", []):
                        if owner.get("displayName") == username:
                            user_team = team
                            break
                    if user_team:
                        break
                if user_team:
                    break
            
            if not user_team:
                continue  # User not in this league
            
            # Get league rules for roster composition
            rules = await fleaflicker_client.fetch_league_rules(league_id)
            
            # Parse roster positions from rules
            roster_positions = rules.get("rosterPositions", [])
            
            # Count position slots
            qbs = sum(1 for p in roster_positions if p.get("position") == "QB")
            rbs = sum(1 for p in roster_positions if p.get("position") == "RB")
            wrs = sum(1 for p in roster_positions if p.get("position") == "WR")
            tes = sum(1 for p in roster_positions if p.get("position") == "TE")
            flexes = sum(1 for p in roster_positions if "FLEX" in p.get("position", "") and "SUPER" not in p.get("position", ""))
            super_flexes = sum(1 for p in roster_positions if "SUPER_FLEX" in p.get("position", ""))
            rec_flexes = sum(1 for p in roster_positions if "REC_FLEX" in p.get("position", ""))
            
            starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])
            total_roster = len(roster_positions)
            
            # Determine league category (1=standard, 2=superflex)
            league_cat = 2 if super_flexes > 0 else 1
            
            league_name = standings.get("league", {}).get("name", f"League {league_id}")
            
            normalized_leagues.append((
                league_name,
                str(league_id),
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
                league_cat,  # Integer: 1=standard, 2=superflex
                season,
                None,  # No previous season ID
            ))
            
        except Exception as e:
            print(f"Error fetching league {league_id}: {e}")
            continue
    
    return normalized_leagues


# @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily 
async def get_fleaflicker_user_leagues_by_email(email: str, season: str, timestamp: str = None) -> Tuple[str, List[Dict]]:
    """
    Get leagues for a Fleaflicker user using their email address.
    
    Args:
        email: User's email address
        season: Season year  
        timestamp: Optional timestamp for cache busting
        
    Returns:
        Tuple of (user_id, list of league dictionaries)
    """
    try:
        print(f"DEBUG: Attempting Fleaflicker email lookup for: {email}, season: {season}")
        
        # Try the requested season first
        user_leagues_data = await fleaflicker_client.get_user_leagues(None, season, email)
        
        # If no leagues found and we're looking for 2024, also try 2025
        if not user_leagues_data and season == "2024":
            print(f"DEBUG: No leagues found for {season}, trying 2025...")
            user_leagues_data = await fleaflicker_client.get_user_leagues(None, "2025", email)
            if user_leagues_data:
                print(f"DEBUG: Found leagues in 2025 season")
                season = "2025"  # Update season for the returned data
        
        print(f"DEBUG: Fleaflicker API response for email {email}: {user_leagues_data}")
        
        if not user_leagues_data:
            print(f"DEBUG: No leagues returned for email {email}")
            return None, []
        
        # Extract user_id from the response if available
        # We need to get the numeric user ID from the league standings
        user_id = None  # Will be populated from response
        
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
                                        # This is the owner of the user's team
                                        user_id = str(owner.get("id"))
                                        print(f"DEBUG: Found Fleaflicker user ID {user_id} for email {email}")
                                        break
                                if user_id:
                                    break
                            if user_id:
                                break
                    except Exception as e:
                        print(f"DEBUG: Could not extract user ID from standings: {e}")
                
                # Get league rules for roster composition
                rules = await fleaflicker_client.fetch_league_rules(league_id)
                
                # Parse roster positions from rules
                roster_positions = rules.get("rosterPositions", [])
                
                # Count position slots
                qbs = sum(1 for p in roster_positions if p.get("position") == "QB")
                rbs = sum(1 for p in roster_positions if p.get("position") == "RB")
                wrs = sum(1 for p in roster_positions if p.get("position") == "WR")
                tes = sum(1 for p in roster_positions if p.get("position") == "TE")
                flexes = sum(1 for p in roster_positions if "FLEX" in p.get("position", "") and "SUPER" not in p.get("position", ""))
                super_flexes = sum(1 for p in roster_positions if "SUPER_FLEX" in p.get("position", ""))
                rec_flexes = sum(1 for p in roster_positions if "REC_FLEX" in p.get("position", ""))
                
                starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])
                total_roster = len(roster_positions)
                
                # Determine league category (1=standard, 2=superflex)
                league_cat = 2 if super_flexes > 0 else 1
                
                league_name = league.get("name", f"League {league_id}")
                total_teams = league.get("size", 0)
                
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
                    league_cat,  # Integer: 1=standard, 2=superflex
                    season,
                    None,  # No previous season ID
                ))
                
                # Try to extract user_id from league data if not found yet
                if not user_id:
                    # Last resort: check if user_id is in the league response somewhere
                    # But never fall back to email - we want numeric IDs only
                    user_id = league.get("user_id")  # No email fallback
                
            except Exception as e:
                print(f"Error processing league {league.get('id', 'unknown')}: {e}")
                continue
        
        # If we couldn't extract user_id from leagues, we'll return None
        # This forces the calling code to handle the case properly
        if not user_id:
            print(f"WARNING: Could not extract numeric user ID for email {email}")
            return None, []
            
        return user_id, normalized_leagues
        
    except Exception as e:
        print(f"Error fetching user leagues by email {email}: {e}")
        return None, []


# @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily 
async def get_fleaflicker_user_leagues(user_id: str, season: str, email: str = None, timestamp: str = None) -> List[Dict]:
    """
    Get leagues for a Fleaflicker user using the FetchUserLeagues API endpoint.
    
    Args:
        user_id: Fleaflicker user ID (must be numeric)
        season: Season year  
        email: User's email address (optional)
        timestamp: Optional timestamp for cache busting
        
    Returns:
        List of league dictionaries with normalized structure
    """
    try:
        # Validate that user_id is numeric
        try:
            numeric_user_id = int(user_id)
        except ValueError:
            print(f"Fleaflicker user_id must be numeric, got: {user_id}")
            raise ValueError(f"Invalid Fleaflicker user_id: {user_id}. Must be a numeric value.")
        
        # Use the actual FetchUserLeagues endpoint
        user_leagues_data = await fleaflicker_client.get_user_leagues(str(numeric_user_id), season, email)
        
        if not user_leagues_data:
            return []
        
        normalized_leagues = []
        
        for league in user_leagues_data:
            try:
                league_id = str(league.get("id"))
                
                # Get league rules for roster composition
                rules = await fleaflicker_client.fetch_league_rules(league_id)
                
                # Parse roster positions from rules
                roster_positions = rules.get("rosterPositions", [])
                
                # Count position slots
                qbs = sum(1 for p in roster_positions if p.get("position") == "QB")
                rbs = sum(1 for p in roster_positions if p.get("position") == "RB")
                wrs = sum(1 for p in roster_positions if p.get("position") == "WR")
                tes = sum(1 for p in roster_positions if p.get("position") == "TE")
                flexes = sum(1 for p in roster_positions if "FLEX" in p.get("position", "") and "SUPER" not in p.get("position", ""))
                super_flexes = sum(1 for p in roster_positions if "SUPER_FLEX" in p.get("position", ""))
                rec_flexes = sum(1 for p in roster_positions if "REC_FLEX" in p.get("position", ""))
                
                starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])
                total_roster = len(roster_positions)
                
                # Determine league category (1=standard, 2=superflex)
                league_cat = 2 if super_flexes > 0 else 1
                
                league_name = league.get("name", f"League {league_id}")
                total_teams = league.get("size", 0)
                
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
                    league_cat,  # Integer: 1=standard, 2=superflex
                    season,
                    None,  # No previous season ID
                ))
                
            except Exception as e:
                print(f"Error processing league {league.get('id', 'unknown')}: {e}")
                continue
        
        return normalized_leagues
        
    except Exception as e:
        print(f"Error fetching user leagues for user {user_id}: {e}")
        raise


async def clean_fleaflicker_league_data(db, session_id: str, league_id: str):
    """
    Clean existing Fleaflicker league data before refresh.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID
    """
    queries = [
        """
        DELETE FROM dynastr.league_players 
        WHERE session_id = $1 AND league_id = $2;
        """,
        """
        DELETE FROM dynastr.fleaflicker_player_scores 
        WHERE session_id = $1 AND league_id = $2;
        """,
        """
        DELETE FROM dynastr.fleaflicker_scoreboards
        WHERE session_id = $1 AND league_id = $2;
        """,
        """
        DELETE FROM dynastr.fleaflicker_transactions
        WHERE session_id = $1 AND league_id = $2;
        """,
        """
        DELETE FROM dynastr.fleaflicker_teams
        WHERE session_id = $1 AND league_id = $2;
        """,
        """
        DELETE FROM dynastr.fleaflicker_league_metadata
        WHERE session_id = $1 AND league_id = $2;
        """
    ]
    
    async with db.transaction():
        for query in queries:
            await db.execute(query, session_id, league_id)


async def clean_fleaflicker_draft_data(db, session_id: str, league_id: str):
    """
    Clean existing draft picks and positions for Fleaflicker league.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID
    """
    queries = [
        """
        DELETE FROM dynastr.draft_picks 
        WHERE league_id = $1 AND session_id = $2;
        """,
        """
        DELETE FROM dynastr.draft_positions 
        WHERE league_id = $1;
        """
    ]
    
    async with db.transaction():
        # Clean draft picks using both league_id and session_id
        await db.execute(queries[0], league_id, session_id)
        # Clean draft positions using just league_id
        await db.execute(queries[1], league_id)


# @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
async def get_fleaflicker_managers(league_id: str, timestamp: str = None) -> List[List]:
    """
    Get manager/team information for a Fleaflicker league.
    
    Args:
        league_id: Fleaflicker league ID
        timestamp: Optional timestamp for cache busting
        
    Returns:
        List of manager data tuples
    """
    standings = await fleaflicker_client.fetch_league_standings(league_id)
    
    manager_data = []
    # Teams are nested in divisions
    for division in standings.get("divisions", []):
        for team in division.get("teams", []):
            owners = team.get("owners", [])
            for owner in owners:
                # Construct Fleaflicker avatar URL using team ID pattern
                avatar_url = f"https://s3.amazonaws.com/fleaflicker/t{team.get('id')}_0_150x150.jpg"
                
                manager_data.append([
                    "fleaflicker",  # source
                    str(owner.get("id")),  # user_id as string
                    league_id,
                    avatar_url,  # Fleaflicker avatar URL pattern
                    owner.get("displayName", "")
                    # Removed platform column to match Sleeper implementation
                ])
    
    return manager_data


# REMOVED: Player records should come from centralized player load API, not platform-specific inserts
# async def _ensure_fleaflicker_player_exists(db, player_id: str, player_name: str, pro_player: Dict):
#     """
#     Ensure a Fleaflicker player exists in the players table for mapping to rankings.
#     
#     Args:
#         db: Database connection
#         player_id: Fleaflicker player ID
#         player_name: Full player name
#         pro_player: Full pro player data from API
#     """
#     try:
#         # Extract player details from Fleaflicker data
#         position = pro_player.get("position", "UNKNOWN")
#         team = pro_player.get("proTeamAbbreviation", "")
#         
#         # Insert or update player record
#         sql = """
#             INSERT INTO dynastr.players (player_id, full_name, player_position, team)
#             VALUES ($1, $2, $3, $4)
#             ON CONFLICT (player_id) 
#             DO UPDATE SET 
#                 full_name = EXCLUDED.full_name,
#                 player_position = EXCLUDED.player_position,
#                 team = EXCLUDED.team;
#         """
#         
#         await db.execute(sql, player_id, player_name, position, team)
#         print(f"Ensured player record exists: {player_name} ({player_id})")
#         
#     except Exception as e:
#         print(f"Error ensuring player exists {player_name}: {e}")


# DO NOT INSERT INTO dynastr.players - player records should only come from centralized player load API
# async def _create_fleaflicker_player_records(db, player_details: List[Dict]):
#     """
#     REMOVED: Platform integrations should not insert into dynastr.players
#     Player records should only come from a centralized player load API
#     """
#     pass


def _has_players_in_roster(roster_data: Dict) -> bool:
    """Check if roster data contains any players."""
    if not roster_data:
        return False
    
    groups = roster_data.get("groups", [])
    for group in groups:
        if isinstance(group, dict):
            slots = group.get("slots", [])
            for slot in slots:
                if isinstance(slot, dict) and slot.get("leaguePlayer"):
                    return True
    return False


async def get_fleaflicker_league_rosters(league_id: str, season: str = None, timestamp: str = None) -> List[Dict]:
    """
    Get all rosters for a Fleaflicker league by fetching each team's roster individually.
    
    Args:
        league_id: Fleaflicker league ID
        season: Season year
        timestamp: Optional timestamp for cache busting
        
    Returns:
        List of roster dictionaries
    """
    # First get team standings to get all team IDs
    standings = await fleaflicker_client.fetch_league_standings(league_id)
    
    processed_rosters = []
    team_count = 0
    
    # Get roster for each team individually using FetchRoster endpoint
    for division in standings.get("divisions", []):
        for team in division.get("teams", []):
            team_id = team.get("id")
            if not team_id:
                continue
                
            team_count += 1
            print(f"DEBUG: Fetching roster for team {team_id} ({team.get('name', 'Unknown')})")
            
            try:
                print(f"DEBUG: API call params - league_id={league_id}, team_id={team_id}, season={season}")
                
                # Try multiple parameter combinations to get roster data
                roster_data = None
                
                # For keeper leagues in offseason, try 2024 season where keepers are stored
                roster_data = None
                
                # Try 1: 2024 season (completed season with keepers)
                try:
                    print(f"DEBUG: Trying 2024 season for keeper data...")
                    roster_data = await fleaflicker_client.fetch_roster(league_id, str(team_id), "2024")
                    has_players = _has_players_in_roster(roster_data) if roster_data else False
                    if has_players:
                        print(f"DEBUG: SUCCESS: Found players in 2024 season!")
                    else:
                        print(f"DEBUG: No players in 2024 season")
                except Exception as e:
                    print(f"DEBUG: Error with 2024 season: {e}")
                
                # Try 2: Current season (2025) as fallback
                if not _has_players_in_roster(roster_data):
                    try:
                        print(f"DEBUG: Trying current season {season}...")
                        roster_data = await fleaflicker_client.fetch_roster(league_id, str(team_id), season)
                        has_players = _has_players_in_roster(roster_data) if roster_data else False
                        if has_players:
                            print(f"DEBUG: Found players in season {season}")
                        else:
                            print(f"DEBUG: No players in season {season}")
                    except Exception as e:
                        print(f"DEBUG: Error with season {season}: {e}")
                
                # Try 3: No season parameter
                if not _has_players_in_roster(roster_data):
                    try:
                        print(f"DEBUG: Trying no season parameter...")
                        roster_data = await fleaflicker_client.fetch_roster(league_id, str(team_id))
                        has_players = _has_players_in_roster(roster_data) if roster_data else False
                        if has_players:
                            print(f"DEBUG: Found players with no season parameter")
                        else:
                            print(f"DEBUG: No players with no season parameter")
                    except Exception as e:
                        print(f"DEBUG: Error with no season: {e}")
                
                print(f"DEBUG: Team {team_id} roster keys: {list(roster_data.keys()) if roster_data else 'No data'}")
                
                roster = {
                    "owner_id": str(team.get("owners", [{}])[0].get("id")) if team.get("owners") and team.get("owners")[0].get("id") else "UNKNOWN_OWNER",
                    "roster_id": team_id,
                    "team_id": team_id,
                    "team_name": team.get("name"),
                    "league_id": league_id,
                    "players": []
                }
                
                # Extract player IDs from roster response
                if roster_data:
                    # The data is in the 'groups' key based on API response
                    groups = roster_data.get("groups", [])
                    
                    print(f"DEBUG: Team {team_id} has {len(groups)} groups")
                    
                    for group in groups:
                        if isinstance(group, dict):
                            slots = group.get("slots", [])
                            print(f"DEBUG: Group has {len(slots)} slots")
                            
                            for slot_idx, slot in enumerate(slots):
                                if isinstance(slot, dict):
                                    # The player data is in 'leaguePlayer', not 'player'
                                    league_player = slot.get("leaguePlayer")
                                    if league_player and isinstance(league_player, dict):
                                        pro_player = league_player.get("proPlayer")
                                        if pro_player and isinstance(pro_player, dict):
                                            player_id = str(pro_player.get("id", ""))
                                            player_name = pro_player.get("nameFull", "Unknown")
                                            is_keeper = league_player.get("isKeeper", False)
                                            
                                            if player_id and player_id != "" and player_id != "None":
                                                roster["players"].append(player_id)
                                                # Simplified logging for production
                                                keeper_status = " (KEEPER)" if is_keeper else ""
                                                print(f"Found player {player_name} (ID: {player_id}){keeper_status}")
                                                
                                                # Store player data for later insertion
                                                roster["player_details"] = roster.get("player_details", [])
                                                roster["player_details"].append({
                                                    "player_id": player_id,
                                                    "player_name": player_name,
                                                    "pro_player": pro_player
                                                })
                                    else:
                                        print(f"DEBUG: Empty slot {slot_idx} on team {team_id}: keys = {list(slot.keys())}")
                
                print(f"DEBUG: Team {team_id} has {len(roster['players'])} players")
                processed_rosters.append(roster)
                
            except Exception as e:
                print(f"DEBUG: Error fetching roster for team {team_id}: {e}")
                continue
    
    print(f"DEBUG: Processed {len(processed_rosters)} rosters from {team_count} teams")
    return processed_rosters

async def insert_fleaflicker_ranks_summary(db, session_id: str, league_id: str, rank_source: str = 'ktc'):
    """
    Calculate and insert power rankings for a Fleaflicker league.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID  
        rank_source: Ranking source (ktc, fc, sf, dp, dd)
    """
    try:
        # Get league summary data to calculate rankings
        # Need to join with fleaflicker_teams to get proper user mapping
        summary_query = f"""
            WITH team_values AS (
                SELECT 
                    ft.owner_id as user_id,
                    ft.owner_display_name as display_name,
                    COALESCE(SUM(CASE 
                        WHEN p.player_position IN ('QB', 'RB', 'WR', 'TE') 
                        THEN sfr.superflex_sf_value 
                        ELSE 0 
                    END), 0) as total_value,
                    COALESCE(SUM(CASE 
                        WHEN p.player_position IN ('QB', 'RB', 'WR', 'TE')
                        THEN sfr.superflex_sf_value 
                        ELSE 0 
                    END), 0) as starters_value,
                    COALESCE(SUM(CASE 
                        WHEN dp.owner_id IS NOT NULL 
                        THEN sfr2.superflex_sf_value 
                        ELSE 0 
                    END), 0) as picks_value
                FROM dynastr.fleaflicker_teams ft
                LEFT JOIN dynastr.league_players lp ON ft.owner_id = lp.user_id 
                    AND lp.league_id = $1 AND lp.session_id = $2
                LEFT JOIN dynastr.players p ON lp.player_id = p.player_id
                LEFT JOIN dynastr.sf_player_ranks sfr ON p.full_name = sfr.player_full_name 
                    AND sfr.rank_type = 'dynasty'
                LEFT JOIN dynastr.draft_picks dp ON ft.team_id = dp.owner_id 
                    AND dp.league_id = $1 AND dp.session_id = $2
                LEFT JOIN dynastr.sf_player_ranks sfr2 ON (dp.year || ' ' || dp.round_name) = sfr2.player_full_name
                    AND sfr2.rank_type = 'dynasty'
                WHERE ft.league_id = $1 AND ft.session_id = $2
                GROUP BY ft.owner_id, ft.owner_display_name
            )
            SELECT 
                user_id,
                display_name,
                total_value,
                RANK() OVER (ORDER BY total_value DESC) as power_rank,
                starters_value,
                RANK() OVER (ORDER BY starters_value DESC) as starters_rank,
                0 as bench_value,
                1 as bench_rank,
                picks_value,
                RANK() OVER (ORDER BY picks_value DESC) as picks_rank
            FROM team_values
            ORDER BY power_rank
        """
        
        rankings = await db.fetch(summary_query, league_id, session_id)
        
        if not rankings:
            print(f"No rankings data found for league {league_id}")
            return {"status": "error", "message": "No rankings data found"}
        
        # Insert rankings into ranks_summary table
        entry_time = datetime.utcnow()
        
        for rank in rankings:
            insert_sql = f"""
                INSERT INTO dynastr.ranks_summary (
                    user_id, display_name, league_id, 
                    {rank_source}_power_rank, {rank_source}_starters_rank,
                    {rank_source}_bench_rank, {rank_source}_picks_rank, 
                    updatetime
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (user_id, league_id) 
                DO UPDATE SET 
                    updatetime = CURRENT_TIMESTAMP,
                    {rank_source}_power_rank = EXCLUDED.{rank_source}_power_rank,
                    {rank_source}_starters_rank = EXCLUDED.{rank_source}_starters_rank,
                    {rank_source}_bench_rank = EXCLUDED.{rank_source}_bench_rank,
                    {rank_source}_picks_rank = EXCLUDED.{rank_source}_picks_rank
            """
            
            await db.execute(
                insert_sql,
                rank['user_id'],
                rank['display_name'],
                league_id,
                rank['power_rank'],
                rank['starters_rank'],
                rank['bench_rank'],
                rank['picks_rank'],
                entry_time
            )
        
        print(f"Successfully inserted {len(rankings)} rankings for league {league_id} using {rank_source}")
        return {"status": "success", "rankings_inserted": len(rankings)}
        
    except Exception as e:
        print(f"Error inserting Fleaflicker ranks summary: {e}")
        raise


async def insert_fleaflicker_league_rosters(db, session_id: str, user_id: str, league_id: str, season: str = None, timestamp: str = None):
    """
    Insert Fleaflicker roster data into league_players table.
    
    Args:
        db: Database connection
        session_id: Session ID
        user_id: User ID (requester)
        league_id: League ID
        timestamp: Optional timestamp for cache busting
    """
    entry_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    rosters = await get_fleaflicker_league_rosters(league_id, season, timestamp=timestamp)
    
    print(f"DEBUG: Roster API returned {len(rosters)} rosters")
    if rosters:
        print(f"DEBUG: First roster sample: {rosters[0]}")
        # Check if rosters have players
        for i, roster in enumerate(rosters[:2]):  # Show first 2 rosters
            players = roster.get("players", [])
            print(f"DEBUG: Roster {i} has {len(players)} players")
            if players:
                print(f"DEBUG: First few players: {players[:3]}")
    else:
        print("DEBUG: No rosters returned - checking if API needs different parameters")
    
    # First, ensure all Fleaflicker players exist in the players table
    all_player_details = []
    for roster in rosters:
        if "player_details" in roster:
            all_player_details.extend(roster["player_details"])
    
    # DO NOT INSERT INTO dynastr.players - player records should only come from centralized player load API
    # Platform integrations should only track player ownership in dynastr.league_players
    # if all_player_details:
    #     await _create_fleaflicker_player_records(db, all_player_details)
    
    # Create a mapping of Fleaflicker player IDs to player names
    fleaflicker_to_name_map = {}
    for roster in rosters:
        if "player_details" in roster:
            for detail in roster["player_details"]:
                fleaflicker_to_name_map[detail["player_id"]] = detail["player_name"]
    
    # Look up existing player IDs from dynastr.players by name
    player_name_to_id_map = {}
    if fleaflicker_to_name_map:
        # Get all unique player names
        player_names = list(set(fleaflicker_to_name_map.values()))
        
        # Query dynastr.players to get the correct player IDs by name
        query = """
            SELECT player_id, full_name 
            FROM dynastr.players 
            WHERE full_name = ANY($1)
        """
        
        try:
            results = await db.fetch(query, player_names)
            for row in results:
                player_name_to_id_map[row['full_name']] = row['player_id']
            print(f"DEBUG: Mapped {len(player_name_to_id_map)} players from dynastr.players")
        except Exception as e:
            print(f"DEBUG: Error looking up players: {e}")
    
    league_players = []
    
    for roster in rosters:
        owner_id = roster.get("owner_id", "UNKNOWN_OWNER")
        team_id = roster.get("team_id")
        player_list = roster.get("players", [])
        
        for fleaflicker_player_id in player_list:
            # Map Fleaflicker player ID to the correct player ID in dynastr.players
            player_name = fleaflicker_to_name_map.get(fleaflicker_player_id)
            
            if player_name:
                # Use the mapped player_id from dynastr.players if found
                correct_player_id = player_name_to_id_map.get(player_name)
                
                if correct_player_id:
                    league_players.append((
                        session_id,           # $1: session_id  
                        user_id,             # $2: owner_user_id (requesting user)
                        str(correct_player_id), # $3: player_id from dynastr.players (mapped by name)
                        league_id,           # $4: league_id
                        str(owner_id),       # $5: user_id (ROSTER OWNER)
                        str(entry_time)      # $6: insert_date
                    ))
                else:
                    print(f"DEBUG: No mapping found for player: {player_name} (Fleaflicker ID: {fleaflicker_player_id})")
            else:
                print(f"DEBUG: No name found for Fleaflicker player ID: {fleaflicker_player_id}")
    
    if not league_players:
        return
    
    sql = """
        INSERT INTO dynastr.league_players
        (session_id, owner_user_id, player_id, league_id, user_id, insert_date)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (session_id, player_id, league_id, user_id)
        DO UPDATE SET 
            owner_user_id = EXCLUDED.owner_user_id, 
            insert_date = EXCLUDED.insert_date;
    """
    
    print(f"DEBUG: About to execute SQL with {len(league_players)} records")
    print(f"DEBUG: First record sample: {league_players[0] if league_players else 'No records'}")
    
    async with db.transaction():
        await db.executemany(sql, league_players)


async def insert_fleaflicker_teams(db, session_id: str, league_id: str):
    """
    Insert Fleaflicker team data into fleaflicker_teams table.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID
    """
    standings = await fleaflicker_client.fetch_league_standings(league_id)
    entry_time = datetime.utcnow()
    
    team_records = []
    # Teams are nested in divisions
    for division in standings.get("divisions", []):
        for team in division.get("teams", []):
            owners = team.get("owners", [])
            owner_id = owners[0].get("id") if owners else None
            owner_name = owners[0].get("displayName", "") if owners else ""
            
            
            # Parse points (they're formatted strings)
            points_for = 0.0
            points_against = 0.0
            try:
                points_for = float(team.get("pointsFor", {}).get("formatted", "0"))
                points_against = float(team.get("pointsAgainst", {}).get("formatted", "0"))
            except (ValueError, TypeError):
                pass
            
            # Parse record from formatted string like "0-0"
            record_str = team.get("recordOverall", {}).get("formatted", "0-0")
            wins, losses, ties = 0, 0, 0
            try:
                if "-" in record_str:
                    parts = record_str.split("-")
                    wins = int(parts[0])
                    losses = int(parts[1])
            except (ValueError, IndexError):
                pass
            
            team_records.append((
                str(team.get("id")),
                league_id,
                session_id,
                team.get("name"),
                str(owner_id) if owner_id else None,
                owner_name,
                wins,
                losses,
                ties,
                points_for,
                points_against,
                team.get("recordDivision", {}).get("rank"),  # Division standing
                None,  # playoff_seed not available
                entry_time,
                entry_time
            ))
    
    sql = """
        INSERT INTO dynastr.fleaflicker_teams (
            team_id, league_id, session_id, team_name, owner_id, owner_display_name,
            wins, losses, ties, points_for, points_against, standing, playoff_seed,
            created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (team_id, league_id, session_id)
        DO UPDATE SET
            team_name = EXCLUDED.team_name,
            owner_id = EXCLUDED.owner_id,
            owner_display_name = EXCLUDED.owner_display_name,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            ties = EXCLUDED.ties,
            points_for = EXCLUDED.points_for,
            points_against = EXCLUDED.points_against,
            standing = EXCLUDED.standing,
            playoff_seed = EXCLUDED.playoff_seed,
            updated_at = EXCLUDED.updated_at;
    """
    
    async with db.transaction():
        await db.executemany(sql, team_records)


async def insert_fleaflicker_scoreboards(db, session_id: str, league_id: str, season: str, week: int):
    """
    Insert Fleaflicker scoreboard data for a specific week.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID
        season: Season year
        week: Week number
    """
    scoreboard = await fleaflicker_client.fetch_league_scoreboard(league_id, season, str(week))
    games = scoreboard.get("games", [])
    entry_time = datetime.utcnow()
    
    scoreboard_records = []
    for game in games:
        home_team = game.get("home", {})
        away_team = game.get("away", {})
        
        scoreboard_records.append((
            str(game.get("id")) if game.get("id") else None,
            league_id,
            session_id,
            season,
            week,
            str(home_team.get("id")) if home_team.get("id") else None,  # Convert to string
            str(away_team.get("id")) if away_team.get("id") else None,  # Convert to string
            home_team.get("score", {}).get("value", 0.0),
            away_team.get("score", {}).get("value", 0.0),
            game.get("status", "scheduled"),
            game.get("is_playoffs", False),
            game.get("playoff_round"),
            entry_time,
            entry_time
        ))
    
    sql = """
        INSERT INTO dynastr.fleaflicker_scoreboards (
            fantasy_game_id, league_id, session_id, season, scoring_period,
            home_team_id, away_team_id, home_score, away_score, game_status,
            is_playoffs, playoff_round, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (fantasy_game_id, league_id, session_id)
        DO UPDATE SET
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            game_status = EXCLUDED.game_status,
            updated_at = EXCLUDED.updated_at;
    """
    
    async with db.transaction():
        await db.executemany(sql, scoreboard_records)


# @cache(expire=SHORT_CACHE_EXPIRATION)  # Disabled temporarily
async def get_fleaflicker_transactions(league_id: str, max_pages: int = 5) -> List[Dict]:
    """
    Get recent transactions for a Fleaflicker league.
    
    Args:
        league_id: League ID
        max_pages: Maximum pages to fetch
        
    Returns:
        List of transaction dictionaries
    """
    all_transactions = []
    offset = 0
    
    for _ in range(max_pages):
        trans_data = await fleaflicker_client.fetch_league_transactions(league_id, result_offset=offset)
        transactions = trans_data.get("items", [])
        
        if not transactions:
            break
            
        all_transactions.extend(transactions)
        
        if len(transactions) < 25:  # Default page size
            break
            
        offset += 25
    
    return all_transactions


async def insert_fleaflicker_transactions(db, session_id: str, league_id: str):
    """
    Insert Fleaflicker transaction history.
    
    Args:
        db: Database connection
        session_id: Session ID
        league_id: League ID
    """
    transactions = await get_fleaflicker_transactions(league_id)
    entry_time = datetime.utcnow()
    
    transaction_records = []
    for trans in transactions:
        # Process each transaction type
        trans_type = trans.get("type")
        
        # Handle trades
        if trans_type == "TRADE":
            for team_trans in trans.get("trades", []):
                team_id = team_trans.get("team", {}).get("id")
                
                # Players traded in
                for player in team_trans.get("traded_for", []):
                    transaction_records.append((
                        trans.get("id"),
                        league_id,
                        session_id,
                        "trade",
                        trans.get("status", "executed"),
                        team_id,
                        str(player.get("proPlayer", {}).get("id")),
                        player.get("proPlayer", {}).get("name"),
                        "add",
                        trans.get("timestamp"),
                        trans.get("season"),
                        trans.get("scoring_period"),
                        None,  # details JSONB
                        entry_time
                    ))
                
                # Players traded away
                for player in team_trans.get("traded_away", []):
                    transaction_records.append((
                        trans.get("id"),
                        league_id,
                        session_id,
                        "trade",
                        trans.get("status", "executed"),
                        team_id,
                        str(player.get("proPlayer", {}).get("id")),
                        player.get("proPlayer", {}).get("name"),
                        "drop",
                        trans.get("timestamp"),
                        trans.get("season"),
                        trans.get("scoring_period"),
                        None,
                        entry_time
                    ))
        
        # Handle waivers/free agents
        elif trans_type in ["WAIVER", "FREE_AGENT"]:
            team_id = trans.get("team", {}).get("id")
            
            # Player added
            if trans.get("player_added"):
                player = trans.get("player_added")
                transaction_records.append((
                    trans.get("id"),
                    league_id,
                    session_id,
                    trans_type.lower(),
                    "executed",
                    team_id,
                    str(player.get("proPlayer", {}).get("id")),
                    player.get("proPlayer", {}).get("name"),
                    "add",
                    trans.get("timestamp"),
                    trans.get("season"),
                    trans.get("scoring_period"),
                    None,
                    entry_time
                ))
            
            # Player dropped
            if trans.get("player_dropped"):
                player = trans.get("player_dropped")
                transaction_records.append((
                    trans.get("id"),
                    league_id,
                    session_id,
                    trans_type.lower(),
                    "executed",
                    team_id,
                    str(player.get("proPlayer", {}).get("id")),
                    player.get("proPlayer", {}).get("name"),
                    "drop",
                    trans.get("timestamp"),
                    trans.get("season"),
                    trans.get("scoring_period"),
                    None,
                    entry_time
                ))
    
    if not transaction_records:
        return
    
    sql = """
        INSERT INTO dynastr.fleaflicker_transactions (
            transaction_id, league_id, session_id, transaction_type, status,
            team_id, player_id, player_name, action, transaction_time,
            season, scoring_period, details, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (transaction_id, league_id, session_id, player_id, action)
        DO NOTHING;
    """
    
    async with db.transaction():
        await db.executemany(sql, transaction_records)


async def player_manager_rosters_fleaflicker(db, roster_data):
    """
    Main function to handle Fleaflicker roster refresh.
    
    Args:
        db: Database connection
        roster_data: Roster data model with league info
    """
    session_id = roster_data.guid
    user_id = roster_data.user_id
    league_id = roster_data.league_id
    year_entered = roster_data.league_year
    
    timestamp = getattr(roster_data, 'timestamp', None)
    
    try:
        # Clean existing data including draft picks
        await clean_fleaflicker_league_data(db, session_id, league_id)
        await clean_fleaflicker_draft_data(db, session_id, league_id)
        
        # Insert managers
        managers = await get_fleaflicker_managers(league_id, timestamp)
        sql = """
            INSERT INTO dynastr.managers (source, user_id, league_id, avatar, display_name)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id)
            DO UPDATE SET
                source = EXCLUDED.source,
                league_id = EXCLUDED.league_id,
                avatar = EXCLUDED.avatar,
                display_name = EXCLUDED.display_name;
        """
        async with db.transaction():
            await db.executemany(sql, managers)
        
        # Insert teams
        await insert_fleaflicker_teams(db, session_id, league_id)
        
        # Insert rosters
        await insert_fleaflicker_league_rosters(db, session_id, user_id, league_id, year_entered, timestamp)
        
        # Insert current week scoreboard
        current_week = 1  # This should be dynamic based on current NFL week
        await insert_fleaflicker_scoreboards(db, session_id, league_id, year_entered, current_week)
        
        # Insert transactions
        await insert_fleaflicker_transactions(db, session_id, league_id)
        
        # Insert draft picks for future years (simplified approach)
        print("DEBUG: Getting ALL future draft picks once...")
        
        # Get ALL draft picks data once (FetchTeamPicks returns all future picks)
        all_draft_picks = await extract_all_fleaflicker_draft_picks(league_id)
        
        if all_draft_picks:
            print(f"DEBUG: Found {len(all_draft_picks)} total draft picks across all years")
            
            # Insert ALL draft picks at once (no year loop to avoid duplication)
            await insert_fleaflicker_draft_picks_data(db, session_id, league_id, all_draft_picks)
            print("DEBUG: Inserted all draft picks successfully")
        else:
            print("DEBUG: No draft picks found")
        
        return {"status": "success", "message": "Fleaflicker rosters updated"}
        
    except Exception as e:
        raise Exception(f"Failed to update Fleaflicker rosters: {e}")


async def extract_all_fleaflicker_draft_picks(league_id: str) -> List[Dict]:
    """
    Extract future draft picks from Fleaflicker using FetchTeamPicks API.
    
    The API returns future draft picks with ownedBy.id as the current owner
    and slot.round/season for the pick details.
    
    Args:
        league_id: Fleaflicker league ID
        
    Returns:
        List of all draft pick dictionaries for future years
    """
    try:
        draft_picks = []
        current_year = datetime.now().year
        
        # Get team standings to find all teams in league
        standings = await fleaflicker_client.fetch_league_standings(league_id)
        
        if not standings or "divisions" not in standings:
            print(f"DEBUG: No standings data found for league {league_id}")
            return []
        
        # Extract all team IDs from standings
        team_ids = []
        for division in standings.get("divisions", []):
            for team in division.get("teams", []):
                if team.get("id"):
                    team_ids.append(str(team.get("id")))
        
        print(f"DEBUG: Found {len(team_ids)} teams in league {league_id}")
        print(f"DEBUG: Fetching future draft picks from Fleaflicker API")
        
        # Track unique picks to avoid duplicates
        seen_picks = set()
        
        # Fetch future draft picks for each team using FetchTeamPicks API
        for team_id in team_ids:
            try:
                team_picks = await fleaflicker_client.fetch_team_picks(league_id, team_id)
                print(f"DEBUG: Raw API response for team {team_id}: picks count = {len(team_picks.get('picks', []))}")
                
                if team_picks and "picks" in team_picks:
                    picks = team_picks.get("picks", [])
                    
                    # Debug: Show raw picks for specific team
                    if team_id == "1798697":
                        print(f"DEBUG: Team 1798697 raw picks sample: {picks[:5]}")
                    
                    for pick in picks:
                        # Extract pick details using the correct API structure
                        season = pick.get("season")
                        slot = pick.get("slot", {})
                        round_num = slot.get("round")
                        slot_position = slot.get("position", slot.get("pick", None))  # Try to get pick position within round
                        owned_by = pick.get("ownedBy", {})
                        owner_id = owned_by.get("id")
                        original = pick.get("original", {})
                        # If no original field, assume the team we're querying is the original owner
                        # unless the pick is owned by someone else (then it was traded)
                        original_id = original.get("id") if original else team_id
                        
                        # Debug: print full slot structure for 2026 2nd round picks
                        if season == 2026 and round_num == 2:
                            print(f"DEBUG: 2026 2nd pick - Full slot data: {slot}")
                            print(f"DEBUG: Position in slot: {slot_position}")
                        
                        # Only include picks that are:
                        # 1. Future years (2026+)
                        # 2. First 4 rounds only
                        # 3. Not already processed (avoid duplicates)
                        if (season and round_num and owner_id and
                            season >= current_year + 1 and  # Future years only
                            round_num <= 4):  # First 4 rounds only
                            
                            # Create unique key using original team ID to handle traded picks
                            # This ensures each original pick is only added once
                            pick_key = f"{team_id}_{season}_{round_num}_{original_id}"
                            
                            if pick_key not in seen_picks:
                                seen_picks.add(pick_key)
                                
                                # For Fleaflicker, always use 'Mid' designation for all future picks
                                # This standardizes valuation regardless of actual draft position
                                round_suffix = _get_round_suffix(round_num)
                                round_name = f"Mid {round_suffix}"
                                
                                # Debug logging for important picks
                                if season == 2026 and round_num == 2:
                                    print(f"DEBUG: Added pick - Owner: {owner_id}, Year: {season}, Round: {round_num}")
                                    print(f"       Round name: '{round_name}'")
                                    print(f"       Original team: {original_id}, Current owner: {owner_id}")
                                    if original_id != owner_id:
                                        print(f"       TRADED PICK - {season} {round_name} from team {original_id} to team {owner_id}")
                                
                                pick_data = {
                                    'year': str(season),
                                    'round': str(round_num),
                                    'round_name': round_name,
                                    'roster_id': str(original_id),  # Original team that had the pick
                                    'owner_id': str(owner_id),      # Current owner (after trades)
                                    'league_id': league_id
                                }
                                draft_picks.append(pick_data)
                                
                                # Log traded picks specially
                                if str(owner_id) != str(original_id):
                                    print(f"DEBUG: TRADED PICK - {season} {round_name} from team {original_id} to team {owner_id}")
                                else:
                                    print(f"DEBUG: Added pick - Owner: {owner_id}, Year: {season}, Round: {round_num}")
                
            except Exception as e:
                print(f"DEBUG: Error fetching picks for team {team_id}: {e}")
                continue
        
        print(f"DEBUG: Extracted {len(draft_picks)} future draft picks from Fleaflicker API")
        return draft_picks
        
    except Exception as e:
        print(f"ERROR: Failed to create draft picks: {e}")
        return []


async def extract_fleaflicker_draft_picks(league_id: str, season: str = "2025") -> List[Dict]:
    """
    Extract draft picks from Fleaflicker using FetchTeamPicks API for each team
    
    Args:
        league_id: Fleaflicker league ID
        season: Draft season (default: 2025)
        
    Returns:
        List of draft pick dictionaries with structure:
        [
            {
                'year': '2025',
                'round': '1', 
                'round_name': '1st',
                'roster_id': '12345',
                'owner_id': '67890',
                'league_id': '443658'
            }
        ]
    """
    try:
        draft_picks = []
        
        # Get team standings to find all teams in league
        standings = await fleaflicker_client.fetch_league_standings(league_id)
        
        if not standings or "divisions" not in standings:
            print(f"DEBUG: No standings data found for league {league_id}")
            return []
        
        # Extract all team IDs from standings
        team_ids = []
        for division in standings.get("divisions", []):
            for team in division.get("teams", []):
                if team.get("id"):
                    team_ids.append(str(team.get("id")))
        
        print(f"DEBUG: Found {len(team_ids)} teams in league {league_id}")
        print(f"DEBUG: Season={season}, League={league_id}")
        
        # Fetch future draft picks for each team using FetchTeamPicks API
        for team_id in team_ids:
            try:
                print(f"DEBUG: Fetching picks for team {team_id}")
                team_picks = await fleaflicker_client.fetch_team_picks(league_id, team_id)
                print(f"DEBUG: Raw API response for team {team_id}: {team_picks}")  # Full API response
                
                if team_picks and "picks" in team_picks:
                    picks = team_picks["picks"]
                    
                    for pick in picks:
                        # Extract pick details
                        pick_season = str(pick.get("season", season))
                        pick_round = pick.get("round", 1)  # This might be wrong!
                        round_name = _get_round_suffix(pick_round)
                        
                        # DEBUG: Print what Fleaflicker API actually returns
                        print(f"DEBUG PICK: team={team_id}, season={pick_season}, round={pick_round}, raw_pick={pick}")
                        
                        # Only include picks for the exact requested season
                        if pick_season == season:
                            draft_pick = {
                                'year': pick_season,
                                'round': str(pick_round),
                                'round_name': round_name,
                                'roster_id': team_id,
                                'owner_id': team_id,  # Current owner (may have been traded)
                                'league_id': league_id
                            }
                            draft_picks.append(draft_pick)
                            print(f"ADDED PICK: {draft_pick}")
                            
                    print(f"DEBUG: Found {len(picks)} API picks for team {team_id}, added {len([p for p in draft_picks if p['owner_id'] == team_id])} to results")
                else:
                    print(f"DEBUG: No picks data returned for team {team_id}")
                    
            except Exception as e:
                print(f"DEBUG: Error fetching picks for team {team_id}: {e}")
                continue
        
        print(f"DEBUG: Extracted {len(draft_picks)} total draft picks for season {season}+")
        return draft_picks
        
    except Exception as e:
        print(f"ERROR: Failed to extract draft picks: {e}")
        return []


async def insert_fleaflicker_draft_picks_data(db, session_id: str, league_id: str, draft_picks_data: List[Dict]):
    """
    Insert draft picks data into database
    
    Args:
        db: Database connection
        session_id: Session identifier 
        league_id: League ID
        draft_picks_data: List of draft pick dictionaries
    """
    if not draft_picks_data:
        print(f"DEBUG: No draft picks data to insert")
        return
        
    print(f"DEBUG: About to insert {len(draft_picks_data)} draft picks into database")
    print(f"DEBUG: Sample picks: {draft_picks_data[:5]}")  # Show first 5 picks
        
    picks_for_db = []
    for pick in draft_picks_data:
        picks_for_db.append([
            pick['year'],
            pick['round'], 
            pick['round_name'],
            pick['roster_id'],
            pick['owner_id'],
            league_id,
            None,  # draft_id (not available from Fleaflicker)
            session_id
        ])
    
    # Insert into database
    sql = """
        INSERT INTO dynastr.draft_picks (year, round, round_name, roster_id, owner_id, league_id, draft_id, session_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (year, round, roster_id, owner_id, league_id, session_id)
        DO UPDATE SET round_name = EXCLUDED.round_name, draft_id = EXCLUDED.draft_id;
    """
    
    async with db.transaction():
        await db.executemany(sql, picks_for_db)
        
    print(f"DEBUG: Inserted {len(picks_for_db)} draft picks for league {league_id}")


def _get_round_suffix(round_num: int) -> str:
    """Get ordinal suffix for round number (1st, 2nd, 3rd, etc.)"""
    if round_num in [11, 12, 13]:
        return f"{round_num}th"
    elif round_num % 10 == 1:
        return f"{round_num}st" 
    elif round_num % 10 == 2:
        return f"{round_num}nd"
    elif round_num % 10 == 3:
        return f"{round_num}rd"
    else:
        return f"{round_num}th"


def _get_positional_round_name(round_num: int, slot: int, league_size: int = 12) -> str:
    """
    Get round name with positional qualifier based on draft slot within the round
    
    Args:
        round_num: Round number (1, 2, 3, etc.)
        slot: Pick position within the round (1-12 for 12-team league)
        league_size: Number of teams in league
        
    Returns:
        String like "Early 1st", "Mid 2nd", "Late 3rd", etc.
    """
    # Get basic ordinal suffix
    if round_num in [11, 12, 13]:
        ordinal = f"{round_num}th"
    elif round_num % 10 == 1:
        ordinal = f"{round_num}st" 
    elif round_num % 10 == 2:
        ordinal = f"{round_num}nd"
    elif round_num % 10 == 3:
        ordinal = f"{round_num}rd"
    else:
        ordinal = f"{round_num}th"
    
    # Determine positional qualifier based on slot within round
    # For 12-team league: 1-4 = Early, 5-8 = Mid, 9-12 = Late
    third = league_size // 3
    if slot <= third:
        return f"Early {ordinal}"
    elif slot <= 2 * third:
        return f"Mid {ordinal}"
    else:
        return f"Late {ordinal}"


async def insert_fleaflicker_draft_picks(db, session_id: str, league_id: str, year: str = "2025"):
    """
    Insert Fleaflicker draft picks into the database
    
    Args:
        db: Database connection
        session_id: Session identifier
        league_id: Fleaflicker league ID 
        year: Draft year (default: 2025)
    """
    try:
        # Note: Draft picks cleanup is now done upfront in clean_fleaflicker_draft_data()
        
        # Extract draft picks from Fleaflicker
        draft_picks_data = await extract_fleaflicker_draft_picks(league_id, year)
        
        if not draft_picks_data:
            print(f"DEBUG: No draft picks found for league {league_id}")
            return
        
        # Prepare data for database insertion
        picks_for_db = []
        for pick in draft_picks_data:
            picks_for_db.append([
                pick['year'],
                pick['round'], 
                pick['round_name'],
                pick['roster_id'],
                pick['owner_id'],
                league_id,
                None,  # draft_id (not available from Fleaflicker)
                session_id
            ])
        
        # Insert into database
        sql = """
            INSERT INTO dynastr.draft_picks (year, round, round_name, roster_id, owner_id, league_id, draft_id, session_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (year, round, roster_id, owner_id, league_id, session_id)
            DO UPDATE SET round_name = EXCLUDED.round_name, draft_id = EXCLUDED.draft_id;
        """
        
        async with db.transaction():
            await db.executemany(sql, picks_for_db)
            
        print(f"DEBUG: Inserted {len(picks_for_db)} draft picks for league {league_id}")
        
    except Exception as e:
        print(f"ERROR: Failed to insert draft picks: {e}")
        raise


async def insert_fleaflicker_draft_positions(db, league_id: str, year: str = "2025", draft_picks_data: List[Dict] = None):
    """
    Insert Fleaflicker draft positions based on actual draft picks owned by teams
    
    Args:
        db: Database connection
        league_id: Fleaflicker league ID 
        year: Draft year (default: 2025)
        draft_picks_data: Optional pre-fetched draft picks data
    """
    try:
        # Note: Draft positions cleanup is now done upfront in clean_fleaflicker_draft_data()
        
        # Get team standings to map team IDs to user IDs
        standings = await fleaflicker_client.fetch_league_standings(league_id)
        
        if not standings or "divisions" not in standings:
            print(f"DEBUG: No standings data found for league {league_id}")
            return
        
        # Create team mapping for user IDs
        team_to_user = {}
        for division in standings.get("divisions", []):
            for team in division.get("teams", []):
                if team.get("id"):
                    team_id = str(team.get("id"))
                    user_id = str(team.get("owners", [{}])[0].get("id", "") if team.get("owners") else "")
                    team_to_user[team_id] = user_id
        
        # Get actual draft picks for this year and create positions based on them
        if draft_picks_data is None:
            draft_picks_data = await extract_fleaflicker_draft_picks(league_id, year)
        
        # Create draft positions based on actual picks owned
        draft_positions = []
        
        # Group picks by round to assign positions within each round
        picks_by_round = {}
        for pick in draft_picks_data:
            if pick['year'] == year:
                round_num = int(pick['round'])
                if round_num not in picks_by_round:
                    picks_by_round[round_num] = []
                picks_by_round[round_num].append(pick)
        
        # Create positions for each round
        for round_num in sorted(picks_by_round.keys()):
            picks_in_round = picks_by_round[round_num]
            
            for i, pick in enumerate(picks_in_round, 1):
                position_name = "Early" if i <= 4 else "Mid" if i <= 8 else "Late"
                user_id = team_to_user.get(pick['roster_id'], "")
                
                draft_positions.append([
                    year,                    # season
                    str(round_num),          # rounds - USE ACTUAL ROUND NUMBER!
                    str(i),                  # position (within this round)
                    position_name,           # position_name
                    pick['roster_id'],       # roster_id
                    user_id,                 # user_id  
                    league_id,               # league_id
                    None,                    # draft_id (not available from Fleaflicker)
                    'N'                      # draft_set_flg (N = not set yet)
                ])
        
        # Only insert if we have positions to insert
        if draft_positions:
            # Insert into database
            sql = """
                INSERT INTO dynastr.draft_positions (season, rounds, position, position_name, roster_id, user_id, league_id, draft_id, draft_set_flg)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (season, rounds, position, user_id, league_id)
                DO UPDATE SET position_name = EXCLUDED.position_name,
                              roster_id = EXCLUDED.roster_id,
                              draft_id = EXCLUDED.draft_id,
                              draft_set_flg = EXCLUDED.draft_set_flg;
            """
            
            async with db.transaction():
                await db.executemany(sql, draft_positions)
                
            print(f"DEBUG: Inserted {len(draft_positions)} draft positions for league {league_id} (year {year})")
        else:
            print(f"DEBUG: No draft positions to insert for league {league_id} (year {year})")
        
    except Exception as e:
        print(f"ERROR: Failed to insert draft positions: {e}")
        raise