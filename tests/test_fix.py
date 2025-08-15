#!/usr/bin/env python3

import asyncio
import sys
import os

# Add current directory to Python path
sys.path.insert(0, '/Users/glstream/Documents/project-folder/fantasy_navigator_services/fn_app_backend')

from fleaflicker.fleaflicker_utils import update_fleaflicker_league_data

async def test_fix():
    """Test the draft pick naming fix"""
    
    # Test with the problematic league
    league_id = "443658" 
    session_id = "fleaflicker_session"
    
    try:
        print("Testing draft pick value fix...")
        print(f"Updating league {league_id}...")
        
        result = await update_fleaflicker_league_data(session_id, league_id)
        
        if result.get("status") == "success":
            print("✅ League data updated successfully!")
            print("The draft pick naming fix should now be in effect.")
            print("Please check the Fantasy Navigator UI to see if team values are now in the expected 50-75k range instead of 800k+")
        else:
            print(f"❌ Error updating league data: {result}")
            
    except Exception as e:
        print(f"❌ Exception during update: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_fix())