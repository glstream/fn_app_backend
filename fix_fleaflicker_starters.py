#!/usr/bin/env python3
"""
Script to fix Fleaflicker leagues that have starter_cnt = 0
"""
import asyncio
import asyncpg
import os

async def fix_fleaflicker_starters():
    """Fix Fleaflicker leagues with starter_cnt = 0"""
    
    try:
        db_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_CONNECTION_STRING')
        if not db_url:
            print("ERROR: No database URL found. Please set DATABASE_URL or POSTGRES_CONNECTION_STRING environment variable.")
            return
            
        conn = await asyncpg.connect(db_url)
        
        # Find Fleaflicker leagues with starter_cnt = 0
        query = """
            SELECT 
                session_id,
                league_id,
                league_name,
                total_rosters,
                starter_cnt,
                qb_cnt,
                rb_cnt,
                wr_cnt,
                te_cnt,
                flex_cnt,
                sf_cnt
            FROM dynastr.current_leagues 
            WHERE platform = 'fleaflicker' 
            AND starter_cnt = 0
        """
        
        leagues = await conn.fetch(query)
        
        if not leagues:
            print("No Fleaflicker leagues found with starter_cnt = 0")
            await conn.close()
            return
            
        print(f"Found {len(leagues)} Fleaflicker leagues with starter_cnt = 0:")
        
        for league in leagues:
            print(f"  - {league['league_name']} (ID: {league['league_id']}) - {league['total_rosters']} teams")
        
        # Ask for confirmation
        response = input(f"\nUpdate these {len(leagues)} leagues with default starter counts? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled.")
            await conn.close()
            return
        
        # Update each league with reasonable defaults
        updated_count = 0
        
        for league in leagues:
            total_rosters = league['total_rosters'] or 10
            
            # Use reasonable defaults based on league size
            qb_cnt = 1
            rb_cnt = 2  
            wr_cnt = 2
            te_cnt = 1
            flex_cnt = 1
            sf_cnt = 1 if total_rosters >= 10 else 0  # SuperFlex for larger leagues
            
            starter_cnt = qb_cnt + rb_cnt + wr_cnt + te_cnt + flex_cnt + sf_cnt
            
            update_query = """
                UPDATE dynastr.current_leagues 
                SET 
                    qb_cnt = $1,
                    rb_cnt = $2,
                    wr_cnt = $3,
                    te_cnt = $4,
                    flex_cnt = $5,
                    sf_cnt = $6,
                    starter_cnt = $7
                WHERE session_id = $8 AND league_id = $9
            """
            
            await conn.execute(
                update_query, 
                qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt, starter_cnt,
                league['session_id'], league['league_id']
            )
            
            print(f"✓ Updated {league['league_name']}: QB:{qb_cnt} RB:{rb_cnt} WR:{wr_cnt} TE:{te_cnt} FLEX:{flex_cnt} SF:{sf_cnt} = {starter_cnt} starters")
            updated_count += 1
        
        await conn.close()
        print(f"\n✅ Successfully updated {updated_count} Fleaflicker leagues!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(fix_fleaflicker_starters())