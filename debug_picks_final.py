import asyncio
import asyncpg

async def debug_draft_picks_final():
    """Comprehensive debug of draft pick values"""
    
    # Connect to database using connection details
    conn = await asyncpg.connect(
        host="dynasty.postgres.database.azure.com",
        database="postgres", 
        user="dynasty1",
        password="jeKlim@361!9",
        ssl="require"
    )
    
    try:
        print("=== COMPREHENSIVE DRAFT PICK DEBUG ===\n")
        
        # 1. Check how many draft picks exist for SveetVilliam's team
        print("1. Draft picks count for SveetVilliam:")
        rows = await conn.fetch("""
            SELECT COUNT(*) as pick_count, dp.year
            FROM dynastr.draft_picks dp
            INNER JOIN dynastr.managers m ON dp.owner_id = m.roster_id
            WHERE dp.session_id = 'fleaflicker_session'
            AND dp.league_id = '443658'
            AND m.display_name = 'SveetVilliam'
            GROUP BY dp.year
            ORDER BY dp.year
        """)
        
        for row in rows:
            print(f"  {row[1]}: {row[0]} draft picks")
        
        # 2. Check draft positions for SveetVilliam
        print("\n2. Draft positions for SveetVilliam:")
        rows = await conn.fetch("""
            SELECT COUNT(*) as pos_count, dpos.season, dpos.draft_set_flg
            FROM dynastr.draft_positions dpos
            INNER JOIN dynastr.managers m ON dpos.user_id = m.user_id
            WHERE dpos.league_id = '443658'
            AND m.display_name = 'SveetVilliam'
            GROUP BY dpos.season, dpos.draft_set_flg
            ORDER BY dpos.season
        """)
        
        for row in rows:
            print(f"  {row[1]}: {row[0]} positions (draft_set_flg: {row[2]})")
        
        # 3. Let's see what draft pick names are actually being generated
        print("\n3. Generated draft pick names for SveetVilliam:")
        rows = await conn.fetch("""
            SELECT 
                CASE WHEN (dname.position::integer) < 5 and al.draft_set_flg = 'Y' and al.year = dname.season
                        THEN al.year || ' Early ' || al.round_name
                    WHEN (dname.position::integer) < 9 and al.draft_set_flg = 'Y' and al.year = dname.season
                        THEN al.year || ' Mid ' || al.round_name
                    WHEN (dname.position::integer) >= 9 and al.draft_set_flg = 'Y' and al.year = dname.season
                        THEN al.year || ' Late ' || al.round_name
                    ELSE al.year|| ' Mid ' || al.round_name 
                    END AS player_full_name,
                al.year,
                al.round_name,
                dname.position,
                dname.draft_set_flg
            FROM (                           
                SELECT dp.roster_id,
                    dp.year,
                    dp.round_name,
                    dp.round,
                    dp.league_id,
                    dpos.user_id,
                    dpos.season,
                    dpos.draft_set_flg
                FROM dynastr.draft_picks dp
                INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                    AND dp.league_id = dpos.league_id
                    AND dp.year = dpos.season
                WHERE dp.session_id = 'fleaflicker_session'
                AND dp.league_id = '443658'
            ) al 
            INNER JOIN dynastr.draft_positions dname ON dname.roster_id = al.roster_id 
                AND dname.league_id = al.league_id
                AND dname.season = al.year
            INNER JOIN dynastr.managers m ON al.user_id = m.user_id
            WHERE m.display_name = 'SveetVilliam'
            ORDER BY al.year, al.round::integer
        """)
        
        for row in rows:
            print(f"  '{row[0]}' ({row[1]} {row[2]}, pos {row[3]}, set: {row[4]})")
        
        # 4. Check values for these generated names
        print("\n4. Values from sf_player_ranks for generated names:")
        generated_names = [row[0] for row in rows]
        
        if generated_names:
            for name in generated_names[:10]:  # Limit to first 10 to avoid spam
                value_rows = await conn.fetch("""
                    SELECT player_full_name, league_type 
                    FROM dynastr.sf_player_ranks 
                    WHERE player_full_name = $1
                    AND rank_type = 'dynasty'
                    LIMIT 1
                """, name)
                
                if value_rows:
                    print(f"  '{name}' -> {value_rows[0][1]}")
                else:
                    print(f"  '{name}' -> NO MATCH")
        
        # 5. Check if there are any very high-value draft picks
        print("\n5. High-value draft picks in sf_player_ranks:")
        rows = await conn.fetch("""
            SELECT player_full_name, league_type 
            FROM dynastr.sf_player_ranks 
            WHERE (player_full_name LIKE '%2025%' OR player_full_name LIKE '%2026%') 
            AND (player_full_name LIKE '%1st%' OR player_full_name LIKE '%2nd%' OR player_full_name LIKE '%3rd%' OR player_full_name LIKE '%Mid%' OR player_full_name LIKE '%Early%' OR player_full_name LIKE '%Late%')
            AND rank_type = 'dynasty'
            AND league_type > 1000
            ORDER BY league_type DESC
            LIMIT 10
        """)
        
        print("  High-value draft picks (>1000):")
        for row in rows:
            print(f"    '{row[0]}' -> {row[1]}")
        
        # 6. Let's count total picks value being summed for SveetVilliam
        print("\n6. Total picks calculation for SveetVilliam:")
        rows = await conn.fetch("""
            WITH base_picks as (
                SELECT  
                    al.user_id,
                    CASE WHEN (dname.position::integer) < 5 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Early ' || al.round_name
                        WHEN (dname.position::integer) < 9 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Mid ' || al.round_name
                        WHEN (dname.position::integer) >= 9 and al.draft_set_flg = 'Y' and al.year = dname.season
                            THEN al.year || ' Late ' || al.round_name
                        ELSE al.year|| ' Mid ' || al.round_name 
                        END AS player_full_name 
                FROM (                           
                    SELECT dp.roster_id,
                        dp.year,
                        dp.round_name,
                        dp.round,
                        dp.league_id,
                        dpos.user_id,
                        dpos.season,
                        dpos.draft_set_flg
                    FROM dynastr.draft_picks dp
                    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id 
                        AND dp.league_id = dpos.league_id
                        AND dp.year = dpos.season
                    WHERE dp.session_id = 'fleaflicker_session'
                    AND dp.league_id = '443658'
                ) al 
                INNER JOIN dynastr.draft_positions dname ON dname.roster_id = al.roster_id 
                    AND dname.league_id = al.league_id
                    AND dname.season = al.year
            )
            SELECT 
                COUNT(*) as pick_count,
                COUNT(DISTINCT bp.player_full_name) as unique_names,
                SUM(COALESCE(sf.league_type, 0)) as total_value,
                AVG(COALESCE(sf.league_type, 0)) as avg_value
            FROM base_picks bp
            LEFT JOIN dynastr.sf_player_ranks sf ON bp.player_full_name = sf.player_full_name
            INNER JOIN dynastr.managers m ON bp.user_id = m.user_id
            WHERE sf.rank_type = 'dynasty'
            AND m.display_name = 'SveetVilliam'
        """)
        
        if rows:
            row = rows[0]
            print(f"  Total pick instances: {row[0]}")
            print(f"  Unique pick names: {row[1]}")
            print(f"  Total value: {row[2]}")
            print(f"  Average value per pick: {row[3]}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_draft_picks_final())