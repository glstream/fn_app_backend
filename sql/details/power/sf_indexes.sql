-- Recommended indexes for sf.sql query optimization
-- These indexes should significantly improve query performance

-- Index for league_players filtering (most selective filters first)
CREATE INDEX IF NOT EXISTS idx_league_players_session_league_user 
ON dynastr.league_players (session_id, league_id, user_id);

-- Index for players position filtering
CREATE INDEX IF NOT EXISTS idx_players_position_id 
ON dynastr.players (player_position, player_id);

-- Index for current_leagues lookup
CREATE INDEX IF NOT EXISTS idx_current_leagues_league_session 
ON dynastr.current_leagues (league_id, session_id);

-- Critical index for sf_player_ranks - use ktc_player_id instead of full_name
CREATE INDEX IF NOT EXISTS idx_sf_player_ranks_ktc_rank_type 
ON dynastr.sf_player_ranks (ktc_player_id, rank_type);

-- Index for draft_picks filtering
CREATE INDEX IF NOT EXISTS idx_draft_picks_session_league 
ON dynastr.draft_picks (session_id, league_id, owner_id);

-- Index for draft_positions lookup
CREATE INDEX IF NOT EXISTS idx_draft_positions_roster_league 
ON dynastr.draft_positions (roster_id, league_id);

-- Index for managers lookup
CREATE INDEX IF NOT EXISTS idx_managers_user_id 
ON dynastr.managers (user_id);

-- Composite index for sf_player_ranks with all commonly used columns
CREATE INDEX IF NOT EXISTS idx_sf_player_ranks_comprehensive 
ON dynastr.sf_player_ranks (rank_type, ktc_player_id, league_type, league_pos_col);