#!/usr/bin/env python3
"""
Debug table structures to fix join issue
"""

import asyncio
import asyncpg

async def check_tables():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== MANAGERS TABLE STRUCTURE ===')
    result = await conn.fetch('''
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'dynastr' 
        AND table_name = 'managers' 
        ORDER BY ordinal_position
    ''')
    
    for row in result:
        print(f'{row["column_name"]}: {row["data_type"]}')
    
    print('\n=== SAMPLE MANAGERS DATA (LEAGUE 349505) ===')
    result = await conn.fetch('''
        SELECT * FROM dynastr.managers 
        WHERE league_id = '349505' 
        LIMIT 3
    ''')
    
    for row in result:
        print(dict(row))
    
    print('\n=== DRAFT PICKS TABLE STRUCTURE ===')
    result = await conn.fetch('''
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'dynastr' 
        AND table_name = 'draft_picks' 
        ORDER BY ordinal_position
    ''')
    
    for row in result:
        print(f'{row["column_name"]}: {row["data_type"]}')
    
    print('\n=== SAMPLE DRAFT PICKS DATA (LEAGUE 349505) ===')
    result = await conn.fetch('''
        SELECT * FROM dynastr.draft_picks 
        WHERE league_id = '349505' 
        LIMIT 3
    ''')
    
    for row in result:
        print(dict(row))
    
    print('\n=== CHECK JOIN COMPATIBILITY ===')
    print('Draft picks owner_id values:')
    result = await conn.fetch('''
        SELECT DISTINCT owner_id FROM dynastr.draft_picks 
        WHERE league_id = '349505'
    ''')
    for row in result:
        print(f'  owner_id: {row["owner_id"]}')
    
    print('\nManagers user_id values:')
    result = await conn.fetch('''
        SELECT DISTINCT user_id FROM dynastr.managers 
        WHERE league_id = '349505'
    ''')
    for row in result:
        print(f'  user_id: {row["user_id"]}')
    
    print('\nManagers fields that might match owner_id:')
    result = await conn.fetch('''
        SELECT DISTINCT user_id, team_id, roster_id FROM dynastr.managers 
        WHERE league_id = '349505'
    ''')
    for row in result:
        print(f'  user_id: {row.get("user_id")}, team_id: {row.get("team_id")}, roster_id: {row.get("roster_id")}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_tables())