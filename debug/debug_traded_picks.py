#!/usr/bin/env python3
"""
Debug traded picks to see if SveetVilliam has 2 second round picks
"""

import asyncio
import asyncpg

async def debug_traded_picks():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== CHECKING 2026 2nd ROUND PICKS IN DATABASE ===')
    result = await conn.fetch('''
        SELECT dp.*, ft.owner_display_name, ft.team_name
        FROM dynastr.draft_picks dp
        LEFT JOIN dynastr.fleaflicker_teams ft 
            ON dp.owner_id = ft.team_id 
            AND dp.league_id = ft.league_id
        WHERE dp.league_id = '349505'
            AND dp.year = '2026'
            AND dp.round = '2'
        ORDER BY dp.owner_id
    ''')
    
    print(f'Found {len(result)} total 2026 2nd round picks:')
    for row in result:
        owner_id = row["owner_id"]
        roster_id = row["roster_id"]
        owner_name = row["owner_display_name"] or "Unknown"
        team_name = row["team_name"] or "Unknown"
        print(f'  Pick owned by team {owner_id} ({owner_name} - {team_name}), originally from roster {roster_id}')
    
    print('\n=== CHECKING SVEETVILLIAM (Team 1798697) PICKS ===')
    result = await conn.fetch('''
        SELECT dp.*, ft.owner_display_name
        FROM dynastr.draft_picks dp
        LEFT JOIN dynastr.fleaflicker_teams ft 
            ON dp.owner_id = ft.team_id 
            AND dp.league_id = ft.league_id
        WHERE dp.league_id = '349505'
            AND dp.owner_id = '1798697'
        ORDER BY dp.year, dp.round
    ''')
    
    print(f'SveetVilliam owns {len(result)} total picks:')
    for row in result:
        year = row["year"]
        round_name = row["round_name"]
        roster_id = row["roster_id"]
        print(f'  {year} {round_name} (originally from roster {roster_id})')
    
    print('\n=== CHECKING KNUTE-ROCKNE-ND (Team 1798858) PICKS ===')
    result = await conn.fetch('''
        SELECT dp.*, ft.owner_display_name
        FROM dynastr.draft_picks dp
        LEFT JOIN dynastr.fleaflicker_teams ft 
            ON dp.owner_id = ft.team_id 
            AND dp.league_id = ft.league_id
        WHERE dp.league_id = '349505'
            AND dp.owner_id = '1798858'
        ORDER BY dp.year, dp.round
    ''')
    
    print(f'KNUTE-ROCKNE-ND owns {len(result)} total picks:')
    for row in result:
        year = row["year"]
        round_name = row["round_name"]
        roster_id = row["roster_id"]
        print(f'  {year} {round_name} (originally from roster {roster_id})')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_traded_picks())