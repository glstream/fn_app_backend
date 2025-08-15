#!/usr/bin/env python3
"""
Test the base_picks CTE to see how many picks it returns
"""

import asyncio
import asyncpg

async def test_base_picks():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== TESTING BASE_PICKS CTE ===')
    result = await conn.fetch('''
        WITH base_picks as (
            SELECT DISTINCT 
                ft.owner_id as user_id
                , dp.year as season
                , dp.year 
                , dp.year || ' Mid ' || dp.round_name AS player_full_name 
                , sf.ktc_player_id
            FROM dynastr.draft_picks dp
            INNER JOIN dynastr.fleaflicker_teams ft 
                ON dp.owner_id = ft.team_id 
                AND dp.league_id = ft.league_id
                AND dp.session_id = ft.session_id
            LEFT JOIN dynastr.sf_player_ranks sf 
                ON (dp.year || ' Mid ' || dp.round_name) = sf.player_full_name
                AND sf.rank_type = 'dynasty'
            WHERE dp.league_id = '349505'
                AND dp.session_id = 'f2a27713-e95e-46e5-a27c-02bbcb0a2d09'
                AND CAST(dp.round AS INTEGER) <= 4
        )
        SELECT user_id, player_full_name, COUNT(*) as count
        FROM base_picks
        GROUP BY user_id, player_full_name
        ORDER BY user_id, player_full_name
    ''')
    
    total_picks = sum(row["count"] for row in result)
    print(f'Total picks from base_picks: {total_picks}')
    
    for row in result[:20]:  # Show first 20
        print(f'User {row["user_id"]}: {row["player_full_name"]} ({row["count"]} times)')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_base_picks())