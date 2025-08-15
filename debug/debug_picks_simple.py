import asyncio
import asyncpg

async def debug_draft_picks():
    """Debug draft pick naming and values"""
    
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
        
        # 1. Check what standard draft pick names exist in sf_player_ranks
        print("1. Standard draft pick names in sf_player_ranks table:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE (player_full_name LIKE '%2025%' OR player_full_name LIKE '%2026%') 
            AND (player_full_name LIKE '%1st%' OR player_full_name LIKE '%2nd%' OR player_full_name LIKE '%3rd%' OR player_full_name LIKE '%Mid%' OR player_full_name LIKE '%Early%' OR player_full_name LIKE '%Late%')
            AND rank_type = 'dynasty'
            AND league_type IS NOT NULL
            ORDER BY league_type DESC
            LIMIT 20
        """)
        
        for row in rows:
            print(f"  '{row[0]}' - Value: {row[1]}")
        
        print("\n2. Let's check what exact picks exist in our database:")
        rows = await conn.fetch("""
            SELECT dp.year, dp.round, dp.round_name, dp.owner_id, dp.roster_id
            FROM dynastr.draft_picks dp
            WHERE dp.session_id = 'fleaflicker_session'
            AND dp.league_id = '443658'
            ORDER BY dp.year, dp.round::integer
            LIMIT 10
        """)
        
        print("  Draft picks in database:")
        for row in rows:
            print(f"    {row[0]} {row[2]} (Round {row[1]}) - Owner: {row[3]}, Roster: {row[4]}")
        
        print("\n3. Let's check what draft positions exist:")
        rows = await conn.fetch("""
            SELECT season, position, position_name, roster_id, user_id, draft_set_flg
            FROM dynastr.draft_positions
            WHERE league_id = '443658'
            ORDER BY season, position::integer
            LIMIT 10
        """)
        
        print("  Draft positions in database:")
        for row in rows:
            print(f"    {row[0]} Position {row[1]} ({row[2]}) - Roster: {row[3]}, User: {row[4]}, Set: {row[5]}")
        
        print("\n4. Let's simulate the naming logic from the SQL:")
        rows = await conn.fetch("""
            SELECT 
                dp.year,
                dp.round,
                dp.round_name,
                dpos.position,
                dpos.position_name,
                dpos.draft_set_flg,
                dp.year || ' Mid ' || dp.round_name as generated_name
            FROM dynastr.draft_picks dp
            INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                AND dp.league_id = dpos.league_id
                AND dp.year = dpos.season
            WHERE dp.session_id = 'fleaflicker_session'
            AND dp.league_id = '443658'
            LIMIT 5
        """)
        
        print("  Generated draft pick names:")
        for row in rows:
            print(f"    {row[0]} {row[2]} -> '{row[6]}'")
        
        print("\n5. Check if these generated names match sf_player_ranks:")
        for row in rows:
            generated_name = row[6]
            match_rows = await conn.fetch("""
                SELECT player_full_name, league_type 
                FROM dynastr.sf_player_ranks 
                WHERE player_full_name = $1
                AND rank_type = 'dynasty'
                LIMIT 1
            """, generated_name)
            
            if match_rows:
                print(f"    MATCH: '{generated_name}' -> Value: {match_rows[0][1]}")
            else:
                print(f"    NO MATCH: '{generated_name}'")
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_draft_picks())