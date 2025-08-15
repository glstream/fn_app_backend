#!/usr/bin/env python3
"""
Test the final query to see where duplication happens
"""

import asyncio
import asyncpg

async def test_final_query():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== TESTING FINAL QUERY PICKS SECTION ===')
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
        SELECT DISTINCT
            picks.user_id,
            picks.ktc_player_id,
            picks.player_full_name,
            picks.year as draft_year,
            'PICKS' as player_position,
            'PICKS' as fantasy_position,
            'PICKS' as fantasy_designation,
            CAST(null AS BIGINT) as player_order
        FROM base_picks picks
        ORDER BY picks.user_id, picks.player_full_name
    ''')
    
    print(f'Total distinct picks in UNION section: {len(result)}')
    
    # Now test the join with managers and sf_player_ranks
    print('\n=== TESTING WITH MANAGERS AND SF_PLAYER_RANKS JOINS ===')
    result2 = await conn.fetch('''
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
        SELECT tp.user_id,
               m.display_name,
               tp.picks_player_name as full_name,
               tp.draft_year,
               tp.player_position,
               sf.superflex_one_qb_value as player_value
        FROM (
            SELECT DISTINCT
                user_id,
                null as player_id,
                picks.ktc_player_id,
                picks.player_full_name as picks_player_name,
                picks.year as draft_year,
                'PICKS' as player_position,
                'PICKS' as fantasy_position,
                'PICKS' as fantasy_designation,
                CAST(null AS BIGINT) as player_order
            FROM base_picks picks
        ) tp
        LEFT JOIN dynastr.players p on tp.player_id = p.player_id
        LEFT JOIN dynastr.sf_player_ranks sf on tp.ktc_player_id = sf.ktc_player_id
        INNER JOIN dynastr.managers m on tp.user_id = m.user_id
        WHERE sf.rank_type = 'dynasty'
    ''')
    
    print(f'Total picks after joins: {len(result2)}')
    
    # Group by user to see if there are duplicates per user
    user_counts = {}
    for row in result2:
        user = row["display_name"]
        user_counts[user] = user_counts.get(user, 0) + 1
    
    print('\n=== PICKS PER USER AFTER JOINS ===')
    for user, count in user_counts.items():
        print(f'{user}: {count} picks')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_final_query())