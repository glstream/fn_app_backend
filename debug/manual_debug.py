#!/usr/bin/env python3
"""
Manual debugging script to check draft pick values directly
"""
import asyncio
import asyncpg
import os

async def main():
    # Connect directly to database
    conn = await asyncpg.connect(
        host="dynasty.postgres.database.azure.com",
        database="postgres",
        user="dynasty1",
        password="jeKlim@361!9",
        ssl="require"
    )
    
    print("=== MANUAL DRAFT PICK DEBUG ===\n")
    
    try:
        # 1. First, let's see what sample draft pick values exist in sf_player_ranks
        print("1. Sample draft pick values from sf_player_ranks:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE player_full_name LIKE '%2026%'
            AND player_full_name LIKE '%2nd%'
            AND rank_type = 'dynasty'
            ORDER BY league_type DESC
            LIMIT 5
        """)
        
        total_sample_value = 0
        for row in rows:
            print(f"  '{row[0]}' -> {row[1]}")
            if row[1]:
                total_sample_value += row[1]
        
        print(f"  Total sample value: {total_sample_value}")
        
        # 2. Let's see what the highest value draft picks are
        print("\n2. Highest value draft picks in sf_player_ranks:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE (player_full_name LIKE '%202%' AND (player_full_name LIKE '%1st%' OR player_full_name LIKE '%2nd%' OR player_full_name LIKE '%3rd%' OR player_full_name LIKE '%4th%'))
            AND rank_type = 'dynasty'
            AND league_type IS NOT NULL
            ORDER BY league_type DESC
            LIMIT 10
        """)
        
        for row in rows:
            print(f"  '{row[0]}' -> {row[1]}")
            
        # 3. Let's check if there are any extremely high values
        print("\n3. Draft picks with values > 50,000:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE (player_full_name LIKE '%202%' AND (player_full_name LIKE '%1st%' OR player_full_name LIKE '%2nd%' OR player_full_name LIKE '%3rd%' OR player_full_name LIKE '%4th%'))
            AND rank_type = 'dynasty'
            AND league_type > 50000
            ORDER BY league_type DESC
        """)
        
        if rows:
            print("  Found high-value picks:")
            for row in rows:
                print(f"    '{row[0]}' -> {row[1]}")
        else:
            print("  No draft picks with values > 50,000 found")
        
        # 4. Let's check if there are any with very specific high values that could explain 776,960
        print("\n4. Looking for picks that could explain 776,960 total:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE league_type > 100000
            AND rank_type = 'dynasty'
            ORDER BY league_type DESC
            LIMIT 5
        """)
        
        if rows:
            print("  Found very high value items:")
            for row in rows:
                print(f"    '{row[0]}' -> {row[1]}")
        else:
            print("  No items with values > 100,000 found")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())