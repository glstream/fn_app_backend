
-- Create indexes for the dynastr schema
-- For search conditions in your DELETE and UPDATE operations
CREATE INDEX idx_managers_league_id ON dynastr.managers(league_id);
CREATE INDEX idx_league_players_session_league ON dynastr.league_players(session_id, league_id);
CREATE INDEX idx_draft_picks_league_session ON dynastr.draft_picks(league_id, session_id);
CREATE INDEX idx_draft_positions_league_id ON dynastr.draft_positions(league_id);


-- For your UPSERT operations
CREATE INDEX idx_ranks_summary_user_league ON dynastr.ranks_summary(user_id, league_id);
CREATE INDEX idx_league_players_session_player_league_user ON dynastr.league_players(session_id, player_id, league_id, user_id);


--  Not high tier recommended
CREATE INDEX idx_current_leagues_user_session ON dynastr.current_leagues(user_id, session_id);
CREATE INDEX idx_current_leagues_session_league ON dynastr.current_leagues(session_id, league_id);
CREATE INDEX idx_league_players_combined ON dynastr.league_players(session_id, player_id, league_id, user_id);

CREATE INDEX idx_draft_picks_year_round_owner ON dynastr.draft_picks(year, round, owner_id);

CREATE INDEX idx_player_trades_league_id ON dynastr.player_trades(league_id);
CREATE INDEX idx_player_trades_transaction ON dynastr.player_trades(transaction_id, roster_id);

CREATE INDEX idx_draft_pick_trades_league_id ON dynastr.draft_pick_trades(league_id);
CREATE INDEX idx_draft_pick_trades_season_round ON dynastr.draft_pick_trades(season, round);


-- DROP indexes for dynastr schema
-- DROP INDEX IF EXISTS dynastr.idx_managers_league_id;
-- DROP INDEX IF EXISTS dynastr.idx_league_players_session_league;
-- DROP INDEX IF EXISTS dynastr.idx_draft_picks_league_session;
-- DROP INDEX IF EXISTS dynastr.idx_draft_positions_league_id;

-- DROP INDEX IF EXISTS dynastr.idx_ranks_summary_user_league;
-- DROP INDEX IF EXISTS dynastr.idx_league_players_session_player_league_user;

-- DROP INDEX IF EXISTS dynastr.idx_current_leagues_user_session;
-- DROP INDEX IF EXISTS dynastr.idx_current_leagues_session_league;
-- DROP INDEX IF EXISTS dynastr.idx_league_players_combined;

-- DROP INDEX IF EXISTS dynastr.idx_draft_picks_year_round_owner;

-- DROP INDEX IF EXISTS dynastr.idx_player_trades_league_id;
-- DROP INDEX IF EXISTS dynastr.idx_player_trades_transaction;

-- DROP INDEX IF EXISTS dynastr.idx_draft_pick_trades_league_id;
-- DROP INDEX IF EXISTS dynastr.idx_draft_pick_trades_season_round;