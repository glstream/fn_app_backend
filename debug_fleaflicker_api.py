#!/usr/bin/env python3
"""
Debug script to check what Fleaflicker API returns for league rules.
"""
import asyncio
import json
from fleaflicker.fleaflicker_client import fleaflicker_client

async def debug_fleaflicker_api(league_id: str):
    """Check what Fleaflicker API returns for league rules."""
    
    try:
        print(f"Fetching league rules for league ID: {league_id}")
        rules = await fleaflicker_client.fetch_league_rules(league_id)
        
        print("\n" + "=" * 60)
        print("RAW API RESPONSE:")
        print("=" * 60)
        print(json.dumps(rules, indent=2))
        
        # Parse roster positions
        roster_positions = rules.get("rosterPositions", [])
        print(f"\n\nFound {len(roster_positions)} roster positions:")
        print("-" * 40)
        
        for i, pos in enumerate(roster_positions):
            print(f"{i+1}. {pos}")
        
        # Count positions using the current logic
        print(f"\n\nUsing current parsing logic:")
        print("-" * 40)
        
        qbs = sum(1 for p in roster_positions if p.get("position") == "QB")
        rbs = sum(1 for p in roster_positions if p.get("position") == "RB")
        wrs = sum(1 for p in roster_positions if p.get("position") == "WR")
        tes = sum(1 for p in roster_positions if p.get("position") == "TE")
        flexes = sum(1 for p in roster_positions if "FLEX" in p.get("position", "") and "SUPER" not in p.get("position", ""))
        super_flexes = sum(1 for p in roster_positions if "SUPER_FLEX" in p.get("position", ""))
        rec_flexes = sum(1 for p in roster_positions if "REC_FLEX" in p.get("position", ""))
        
        starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])
        total_roster = len(roster_positions)
        
        print(f"QBs: {qbs}")
        print(f"RBs: {rbs}")
        print(f"WRs: {wrs}")
        print(f"TEs: {tes}")
        print(f"FLEXes: {flexes}")
        print(f"Super FLEXes: {super_flexes}")
        print(f"Rec FLEXes: {rec_flexes}")
        print(f"Total starters: {starters}")
        print(f"Total roster spots: {total_roster}")
        
        # Show all unique position names to help debug
        print(f"\n\nAll unique position values found:")
        print("-" * 40)
        unique_positions = set()
        for pos in roster_positions:
            if isinstance(pos, dict) and "position" in pos:
                unique_positions.add(pos["position"])
        
        for pos in sorted(unique_positions):
            print(f"  '{pos}'")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # You'll need to provide a real Fleaflicker league ID
    league_id = input("Enter Fleaflicker league ID to debug: ").strip()
    if league_id:
        asyncio.run(debug_fleaflicker_api(league_id))
    else:
        print("Please provide a league ID")