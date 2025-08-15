#!/usr/bin/env python3
"""
Debug fleaflicker_teams table to understand team to user mapping
"""

import asyncio
import asyncpg

async def check_fleaflicker_teams():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== FLEAFLICKER_TEAMS TABLE STRUCTURE ===')
    result = await conn.fetch('''
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'dynastr' 
        AND table_name = 'fleaflicker_teams' 
        ORDER BY ordinal_position
    ''')
    
    for row in result:
        print(f'{row["column_name"]}: {row["data_type"]}')
    
    print('\n=== FLEAFLICKER_TEAMS DATA (LEAGUE 349505) ===')
    result = await conn.fetch('''
        SELECT * FROM dynastr.fleaflicker_teams 
        WHERE league_id = '349505' 
    ''')
    
    for row in result:
        print(dict(row))
    
    print('\n=== TEAM ID TO USER ID MAPPING ===')
    result = await conn.fetch('''
        SELECT DISTINCT ft.team_id, ft.owner_id, ft.owner_name, m.user_id, m.display_name
        FROM dynastr.fleaflicker_teams ft
        LEFT JOIN dynastr.managers m ON ft.owner_id = m.user_id AND ft.league_id = m.league_id
        WHERE ft.league_id = '349505'
        ORDER BY ft.team_id
    ''')
    
    for row in result:
        team_id = row["team_id"]
        owner_id = row["owner_id"]
        owner_name = row["owner_name"]
        user_id = row["user_id"]
        display_name = row["display_name"]
        print(f'Team {team_id}: owner_id={owner_id} ({owner_name}) -> user_id={user_id} ({display_name})')
    
    print('\n=== DRAFT PICKS TO TEAM MAPPING ===')
    result = await conn.fetch('''
        SELECT dp.owner_id, dp.year, dp.round_name, ft.owner_id as team_owner_id, ft.owner_name
        FROM dynastr.draft_picks dp
        LEFT JOIN dynastr.fleaflicker_teams ft ON dp.owner_id = ft.team_id AND dp.league_id = ft.league_id
        WHERE dp.league_id = '349505'
        ORDER BY dp.owner_id, dp.year, dp.round
    ''')
    
    for row in result:
        print(f'Pick owner {row["owner_id"]} ({row["year"]} {row["round_name"]}) -> Team owner: {row["team_owner_id"]} ({row["owner_name"]})')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_fleaflicker_teams())