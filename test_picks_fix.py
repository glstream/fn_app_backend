#!/usr/bin/env python3
"""
Test the draft picks fix
"""

import asyncio
import asyncpg

async def test_picks_join():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== TESTING NEW DRAFT PICKS JOIN ===')
    
    # Test the corrected join logic
    result = await conn.fetch('''
        SELECT 
            ft.owner_id as user_id,
            ft.owner_display_name,
            dp.owner_id as team_id,
            ft.team_name,
            dp.year,
            dp.round,
            dp.round_name,
            dp.year || ' Mid ' || dp.round_name AS player_full_name
        FROM dynastr.draft_picks dp
        INNER JOIN dynastr.fleaflicker_teams ft 
            ON dp.owner_id = ft.team_id 
            AND dp.league_id = ft.league_id
            AND dp.session_id = ft.session_id
        WHERE dp.league_id = '349505'
            AND dp.session_id = 'f2a27713-e95e-46e5-a27c-02bbcb0a2d09'
            AND CAST(dp.round AS INTEGER) <= 4
    ''')
    
    print(f'Found {len(result)} draft picks with correct joins:')
    for row in result:
        user_id = row["user_id"]
        name = row["owner_display_name"]
        team_name = row["team_name"]
        year = row["year"]
        round_name = row["round_name"]
        pick_name = row["player_full_name"]
        print(f'  {name} ({user_id}) - {team_name}: {pick_name}')
    
    # Check specific user mentioned in issue (team 1798697 = SveetVilliam)  
    print(f'\n=== SVEETVILLIAM PICKS (Team 1798697) ===')
    result = await conn.fetch('''
        SELECT 
            ft.owner_id as user_id,
            ft.owner_display_name,
            dp.year,
            dp.round_name,
            COUNT(*) as pick_count
        FROM dynastr.draft_picks dp
        INNER JOIN dynastr.fleaflicker_teams ft 
            ON dp.owner_id = ft.team_id 
        WHERE dp.owner_id = '1798697'
            AND dp.league_id = '349505'
            AND dp.session_id = 'f2a27713-e95e-46e5-a27c-02bbcb0a2d09'
        GROUP BY ft.owner_id, ft.owner_display_name, dp.year, dp.round_name
        ORDER BY dp.year, dp.round_name
    ''')
    
    for row in result:
        name = row["owner_display_name"]
        year = row["year"]
        round_name = row["round_name"]
        count = row["pick_count"]
        print(f'  {name}: {year} {round_name} ({count} picks)')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_picks_join())