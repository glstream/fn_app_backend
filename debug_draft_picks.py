import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def debug_draft_picks():
    """Debug draft pick naming and values"""
    
    # Load environment variables
    load_dotenv()
    
    # Connect to database using connection details
    conn = await asyncpg.connect(
        host="dynasty.postgres.database.azure.com",
        database="postgres", 
        user="dynasty1",
        password="jeKlim@361!9",
        ssl="require"
    )
    
    try:
        print("=== DEBUGGING DRAFT PICK VALUES ===\n")
        
        # 1. Check what draft pick names are in sf_player_ranks
        print("1. Draft pick names in sf_player_ranks table:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type, league_pos_col 
            FROM dynastr.sf_player_ranks 
            WHERE (player_full_name LIKE '%2025%' OR player_full_name LIKE '%2026%') 
            AND (player_full_name LIKE '%1st%' OR player_full_name LIKE '%2nd%' OR player_full_name LIKE '%3rd%' OR player_full_name LIKE '%Mid%' OR player_full_name LIKE '%Early%' OR player_full_name LIKE '%Late%')
            AND rank_type = 'dynasty'
            ORDER BY league_type DESC NULLS LAST
            LIMIT 15
        """)
        
        for row in rows:
            print(f"  {row[0]} - SF: {row[1]}, 1QB: {row[2]}")
        
        print("\n2. Draft pick names being generated in current database for Fleaflicker league:")
        rows = await conn.fetch("""
            WITH base_picks as (
                SELECT  
                    al.user_id,
                    al.season,
                    al.year,
                    CASE WHEN (dname.position::integer) < 13 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Round ' || al.round || ' Pick ' || dname.position
                        WHEN (dname.position::integer) > 12 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' ' || dname.position_name || ' ' || al.round_name 
                        ELSE al.year|| ' Mid ' || al.round_name 
                        END AS player_full_name 
                FROM (                           
                    SELECT dp.roster_id,
                        dp.year,
                        dp.round_name,
                        dp.round,
                        dp.league_id,
                        dpos.user_id,
                        dpos.season,
                        dpos.draft_set_flg
                    FROM dynastr.draft_picks dp
                    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                        AND dp.league_id = dpos.league_id
                        AND dp.year = dpos.season
                    WHERE dp.session_id = 'fleaflicker_session'
                    AND dp.league_id = '443658'
                ) al 
                INNER JOIN dynastr.draft_positions dname ON dname.roster_id = al.roster_id 
                    AND dname.league_id = al.league_id
                    AND dname.season = al.year
            )
            SELECT DISTINCT player_full_name 
            FROM base_picks
            LIMIT 10
        """)
        
        print("  Generated draft pick names:")
        for row in rows:
            print(f"    {row[0]}")
        
        print("\n3. Values for these generated names from sf_player_ranks:")
        rows = await conn.fetch("""
            WITH base_picks as (
                SELECT  
                    al.user_id,
                    al.season,
                    al.year,
                    CASE WHEN (dname.position::integer) < 13 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Round ' || al.round || ' Pick ' || dname.position
                        WHEN (dname.position::integer) > 12 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' ' || dname.position_name || ' ' || al.round_name 
                        ELSE al.year|| ' Mid ' || al.round_name 
                        END AS player_full_name 
                FROM (                           
                    SELECT dp.roster_id,
                        dp.year,
                        dp.round_name,
                        dp.round,
                        dp.league_id,
                        dpos.user_id,
                        dpos.season,
                        dpos.draft_set_flg
                    FROM dynastr.draft_picks dp
                    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                        AND dp.league_id = dpos.league_id
                        AND dp.year = dpos.season
                    WHERE dp.session_id = 'fleaflicker_session'
                    AND dp.league_id = '443658'
                ) al 
                INNER JOIN dynastr.draft_positions dname ON dname.roster_id = al.roster_id 
                    AND dname.league_id = al.league_id
                    AND dname.season = al.year
            )
            SELECT bp.player_full_name, sf.dynasty_superflex_value, sf.dynasty_one_qb_value
            FROM base_picks bp
            LEFT JOIN dynastr.sf_player_ranks sf ON bp.player_full_name = sf.player_full_name
            WHERE sf.rank_type = 'dynasty'
            LIMIT 10
        """)
        
        for row in rows:
            print(f"    {row[0]} - SF: {row[1]}, 1QB: {row[2]}")
        
        print("\n4. Total value aggregation:")
        rows = await conn.fetch("""
            WITH base_picks as (
                SELECT  
                    al.user_id,
                    CASE WHEN (dname.position::integer) < 13 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Round ' || al.round || ' Pick ' || dname.position
                        WHEN (dname.position::integer) > 12 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' ' || dname.position_name || ' ' || al.round_name 
                        ELSE al.year|| ' Mid ' || al.round_name 
                        END AS player_full_name 
                FROM (                           
                    SELECT dp.roster_id,
                        dp.year,
                        dp.round_name,
                        dp.round,
                        dp.league_id,
                        dpos.user_id,
                        dpos.season,
                        dpos.draft_set_flg
                    FROM dynastr.draft_picks dp
                    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                        AND dp.league_id = dpos.league_id
                        AND dp.year = dpos.season
                    WHERE dp.session_id = 'fleaflicker_session'
                    AND dp.league_id = '443658'
                ) al 
                INNER JOIN dynastr.draft_positions dname ON dname.roster_id = al.roster_id 
                    AND dname.league_id = al.league_id
                    AND dname.season = al.year
            )
            SELECT bp.user_id, COUNT(*) as pick_count, SUM(COALESCE(sf.dynasty_superflex_value, 0)) as total_value
            FROM base_picks bp
            LEFT JOIN dynastr.sf_player_ranks sf ON bp.player_full_name = sf.player_full_name
            WHERE sf.rank_type = 'dynasty'
            GROUP BY bp.user_id
            ORDER BY total_value DESC
        """)
        
        for row in rows:
            print(f"    User {row[0]}: {row[1]} picks, Total value: {row[2]}")
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_draft_picks())