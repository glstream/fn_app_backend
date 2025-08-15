#!/usr/bin/env python3
"""
Debug if SQL DISTINCT is removing duplicate picks
"""

import asyncio
import asyncpg

async def debug_sql_distinct():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== TESTING BASE_PICKS CTE FOR SVEETVILLIAM 2026 2ND ===')
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
                AND ft.owner_id = '2239577'  -- SveetVilliam
                AND dp.year = '2026' 
                AND dp.round = '2'
        )
        SELECT *
        FROM base_picks
    ''')
    
    print(f'Found {len(result)} picks in base_picks CTE for SveetVilliam 2026 2nd:')
    for row in result:
        print(f'  {dict(row)}')
    
    print('\n=== TESTING WITHOUT DISTINCT IN BASE_PICKS ===')
    result2 = await conn.fetch('''
        WITH base_picks as (
            SELECT 
                ft.owner_id as user_id
                , dp.year as season
                , dp.year 
                , dp.year || ' Mid ' || dp.round_name AS player_full_name 
                , sf.ktc_player_id
                , dp.roster_id  -- Add this to see original team
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
                AND ft.owner_id = '2239577'  -- SveetVilliam
                AND dp.year = '2026' 
                AND dp.round = '2'
        )
        SELECT *
        FROM base_picks
    ''')
    
    print(f'Found {len(result2)} picks without DISTINCT for SveetVilliam 2026 2nd:')
    for row in result2:
        print(f'  User: {row["user_id"]}, Original roster: {row["roster_id"]}, KTC ID: {row["ktc_player_id"]}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_sql_distinct())