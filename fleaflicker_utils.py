"""
Fleaflicker-specific utility functions for data ingestion and processing.
Follows patterns from utils.py for Sleeper integration.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
# from fastapi_cache.decorator import cache  # Disabled temporarily
from fleaflicker_client import fleaflicker_client
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
async def get_fleaflicker_user_leagues(user_id: str, season: str, email: str = None, timestamp: str = None) -> List[Dict]:
    """
    Get leagues for a Fleaflicker user using the FetchUserLeagues API endpoint.
    
    Args:
        user_id: Fleaflicker user ID
        season: Season year  
        email: User's email address (optional)
        timestamp: Optional timestamp for cache busting
        
    Returns:
        List of league dictionaries with normalized structure
    """
    try:
        # Try to convert user_id to integer if it's numeric
        # Fleaflicker likely expects numeric user IDs, not usernames
        try:
            numeric_user_id = int(user_id)
            user_leagues_data = await fleaflicker_client.get_user_leagues(str(numeric_user_id), season)
        except ValueError:
            # If not numeric, treat as username and return empty for now
            print(f"Fleaflicker user_id must be numeric, got: {user_id}")
            return []
        
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
        return []


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


async def _ensure_fleaflicker_player_exists(db, player_id: str, player_name: str, pro_player: Dict):
    """
    Ensure a Fleaflicker player exists in the players table for mapping to rankings.
    
    Args:
        db: Database connection
        player_id: Fleaflicker player ID
        player_name: Full player name
        pro_player: Full pro player data from API
    """
    try:
        # Extract player details from Fleaflicker data
        position = pro_player.get("position", "UNKNOWN")
        team = pro_player.get("proTeamAbbreviation", "")
        
        # Insert or update player record
        sql = """
            INSERT INTO dynastr.players (player_id, full_name, player_position, team)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (player_id) 
            DO UPDATE SET 
                full_name = EXCLUDED.full_name,
                player_position = EXCLUDED.player_position,
                team = EXCLUDED.team;
        """
        
        await db.execute(sql, player_id, player_name, position, team)
        print(f"Ensured player record exists: {player_name} ({player_id})")
        
    except Exception as e:
        print(f"Error ensuring player exists {player_name}: {e}")


async def _create_fleaflicker_player_records(db, player_details: List[Dict]):
    """
    Create player records for all Fleaflicker players to enable mapping to rankings.
    
    Args:
        db: Database connection
        player_details: List of player detail dictionaries
    """
    try:
        player_records = []
        for player_detail in player_details:
            player_id = player_detail["player_id"]
            player_name = player_detail["player_name"]
            pro_player = player_detail["pro_player"]
            
            # Extract player details
            position = pro_player.get("position", "UNKNOWN")
            team = pro_player.get("proTeamAbbreviation", "")
            first_name = pro_player.get("nameFirst", "")
            last_name = pro_player.get("nameLast", "")
            
            player_records.append((player_id, player_name, position, team, first_name, last_name))
        
        if player_records:
            sql = """
                INSERT INTO dynastr.players (player_id, full_name, player_position, team, first_name, last_name)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (player_id) 
                DO UPDATE SET 
                    full_name = EXCLUDED.full_name,
                    player_position = EXCLUDED.player_position,
                    team = EXCLUDED.team,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name;
            """
            
            async with db.transaction():
                await db.executemany(sql, player_records)
            
            print(f"Created/updated {len(player_records)} Fleaflicker player records")
    
    except Exception as e:
        print(f"Error creating Fleaflicker player records: {e}")


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
    
    # Create player records for mapping
    if all_player_details:
        await _create_fleaflicker_player_records(db, all_player_details)
    
    league_players = []
    
    for roster in rosters:
        owner_id = roster.get("owner_id", "UNKNOWN_OWNER")
        team_id = roster.get("team_id")
        player_list = roster.get("players", [])
        
        for player_id in player_list:
            
            league_players.append((
                session_id,           # $1: session_id  
                user_id,             # $2: owner_user_id (requesting user) - MATCH utils.py
                str(player_id),      # $3: player_id (ensure string)
                league_id,           # $4: league_id
                str(owner_id),       # $5: user_id (ROSTER OWNER) - MATCH utils.py
                str(entry_time)      # $6: insert_date (ensure string)
            ))
    
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
        # Clean existing data
        await clean_fleaflicker_league_data(db, session_id, league_id)
        
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
        
        return {"status": "success", "message": "Fleaflicker rosters updated"}
        
    except Exception as e:
        raise Exception(f"Failed to update Fleaflicker rosters: {e}")