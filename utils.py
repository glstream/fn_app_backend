import requests
from requests.exceptions import RequestException
from time import sleep
from psycopg2.extras import execute_batch, execute_values
from superflex_models import UserDataModel, LeagueDataModel, RosterDataModel, RanksDataModel
from datetime import datetime
import asyncio
import aiohttp
import traceback


async def make_api_call(url, params=None, headers=None, timeout=10, max_retries=5, backoff_factor=1):
    async with aiohttp.ClientSession() as session:
        for retry in range(max_retries):
            try:
                async with session.get(url, params=params, headers=headers, timeout=timeout) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                if retry < max_retries - 1:
                    sleep_time = backoff_factor * (2 ** retry)
                    print(f"Error while making API call: {e}. Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"Error while making API call: {e}. Reached maximum retries ({max_retries}).")
                    raise


def dedupe(lst):
    dup_free_set = set()
    for x in lst:
        t = tuple(x)
        if t not in dup_free_set:
            dup_free_set.add(t)
    return list(dup_free_set)


def round_suffix(rank: int) -> str:
    ith = {1: "st", 2: "nd", 3: "rd"}.get(
        rank % 10 * (rank % 100 not in [11, 12, 13]), "th"
    )
    return f"{str(rank)}{ith}"


async def get_user_id(user_name: str) -> str:
    try:
        user_url = f"https://api.sleeper.app/v1/user/{user_name}"
        user_data = await make_api_call(user_url)
        return user_data["user_id"]
    except KeyError:
        raise ValueError(f"User ID not found for user: {user_name}")
    except Exception as e:
        raise ConnectionError(f"Failed to fetch user data: {e}")



async def get_user_name(user_id: str):
    try:
        username_url = f"https://api.sleeper.app/v1/user/{user_id}"
        user_meta = await make_api_call(username_url)
        return (user_meta["username"], user_meta["display_name"])
    except KeyError:
        print(f"Error: Key missing in the response for user {user_id}.")
        return None, None
    except Exception as e:
        print(f"Failed to fetch user data due to: {e}")
        return None, None




async def get_user_leagues(user_name: str, league_year: str) -> list:
    owner_id = await get_user_id(user_name)  # Ensure this call is awaited
    leagues_json = await make_api_call(
        f"https://api.sleeper.app/v1/user/{owner_id}/leagues/nfl/{league_year}"
    )  # Ensure this call is awaited

    leagues = []
    for league in leagues_json:
        qbs = len([i for i in league["roster_positions"] if i == "QB"])
        rbs = len([i for i in league["roster_positions"] if i == "RB"])
        wrs = len([i for i in league["roster_positions"] if i == "WR"])
        tes = len([i for i in league["roster_positions"] if i == "TE"])
        flexes = len([i for i in league["roster_positions"] if i == "FLEX"])
        super_flexes = len([i for i in league["roster_positions"] if i == "SUPER_FLEX"])
        rec_flexes = len([i for i in league["roster_positions"] if i == "REC_FLEX"])
        starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])

        leagues.append(
            (
                league["name"],
                league["league_id"],
                league.get("avatar", ""),
                league["total_rosters"],
                qbs,
                rbs,
                wrs,
                tes,
                flexes,
                super_flexes,
                starters,
                len(league["roster_positions"]),
                league["sport"],
                rec_flexes,
                league["settings"]["type"],
                league_year,
                league.get("previous_league_id", None),
            )
        )
    return leagues



async def clean_league_managers(db, league_id: str):
    delete_query = f"""
        DELETE FROM dynastr.managers 
        WHERE league_id = $1;
    """
    # Use async with to handle the transaction
    async with db.transaction():
        # Execute the delete query asynchronously
        await db.execute(delete_query, league_id)
    return



async def clean_league_rosters(db, session_id: str, league_id: str):
    delete_query = """
        DELETE FROM dynastr.league_players 
        WHERE session_id = $1 AND league_id = $2;
    """
    # Use async with to handle the transaction
    async with db.transaction():
        # Execute the delete query asynchronously
        await db.execute(delete_query, session_id, league_id)
    return


async def clean_league_picks(db, league_id: str, session_id: str) -> None:
    delete_query = """
        DELETE FROM dynastr.draft_picks 
        WHERE league_id = $1 AND session_id = $2;
    """
    # Execute the delete query asynchronously using a transaction
    async with db.transaction():
        await db.execute(delete_query, league_id, session_id)
    return



async def clean_draft_positions(db, league_id: str):
    delete_query = """
        DELETE FROM dynastr.draft_positions 
        WHERE league_id = $1;
    """
    # Execute the delete query asynchronously using a transaction
    async with db.transaction():
        await db.execute(delete_query, league_id)
    return



async def get_managers(league_id: str) -> list:
    url = f"https://api.sleeper.app/v1/league/{league_id}/users"
    res = await make_api_call(url)  # Ensure this call is asynchronous
    manager_data = [
        ["sleeper", i["user_id"], league_id, i.get("avatar", ""), i["display_name"]]
        for i in res
    ]
    return manager_data


async def get_league_rosters_size(league_id: str) -> int:
    url = f"https://api.sleeper.app/v1/league/{league_id}"
    league_res = await make_api_call(url)  # Using the async version of make_api_call
    return league_res["total_rosters"]



async def get_league_rosters(league_id: str) -> list:
    url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
    rosters = await make_api_call(url)
    return rosters

async def get_traded_picks(league_id: str) -> list:
    url = f"https://api.sleeper.app/v1/league/{league_id}/traded_picks"
    total_res = await make_api_call(url)  # Using the async version of make_api_call
    return total_res



async def get_draft_id(league_id: str) -> dict:
    url = f"https://api.sleeper.app/v1/league/{league_id}/drafts"
    draft_res = await make_api_call(url)  # Using the async version of make_api_call
    if draft_res and isinstance(draft_res, list) and len(draft_res) > 0:
        draft_meta = draft_res[0]  # Assume the first draft is what we need
        return draft_meta
    else:
        raise ValueError("No draft data found for the given league ID.")



async def get_draft(draft_id: str):
    draft_res_url = f"https://api.sleeper.app/v1/draft/{draft_id}"
    draft_res = await make_api_call(draft_res_url)
    return draft_res


async def get_roster_ids(league_id: str) -> list:
    try:
        roster_meta_url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
        roster_meta = await make_api_call(roster_meta_url)
        return [(r["owner_id"], str(r["roster_id"])) for r in roster_meta]
    except Exception as e:
        print(f"Failed to fetch or process roster data: {e}")
        return []  # or re-raise the exception depending on how you want to handle errors



async def get_full_league(league_id: str):
    l_res_url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
    l_res = await make_api_call(l_res_url)
    return l_res


async def insert_ranks_summary(db, ranks_data: RanksDataModel):
    user_id = ranks_data.user_id
    display_name = ranks_data.display_name
    league_id = ranks_data.league_id
    rank_source = ranks_data.rank_source
    power_rank = ranks_data.power_rank
    starters_rank = ranks_data.starters_rank
    bench_rank = ranks_data.bench_rank
    picks_rank = ranks_data.picks_rank
    entry_time = datetime.now()

    sql = f"""
        INSERT INTO dynastr.ranks_summary (
            user_id, display_name, league_id, {rank_source}_power_rank, {rank_source}_starters_rank,
            {rank_source}_bench_rank, {rank_source}_picks_rank, updatetime
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (user_id, league_id ) 
        DO UPDATE 
        SET 
            {rank_source}_power_rank = EXCLUDED.{rank_source}_power_rank,
            {rank_source}_starters_rank = EXCLUDED.{rank_source}_starters_rank,
            {rank_source}_bench_rank = EXCLUDED.{rank_source}_bench_rank,
            {rank_source}_picks_rank = EXCLUDED.{rank_source}_picks_rank;
    """

    # Execute the SQL command asynchronously using a transaction
    async with db.transaction():
        await db.execute(sql, user_id, display_name, league_id, power_rank, starters_rank, bench_rank, picks_rank, entry_time)

    return




async def insert_current_leagues(db, user_data: UserDataModel):
    
    user_name = user_data.user_name
    league_year = user_data.league_year
    
    # Execute synchronous code asynchronously
    leagues = await get_user_leagues(user_name, league_year)
    user_id = await get_user_id(user_name)
    
    session_id = user_data.guid
    entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    delete_user_leagues_query = f"""
        DELETE FROM dynastr.current_leagues 
        WHERE user_id = '{user_id}' AND session_id = '{session_id}'
    """
    try:
        # Start a transaction
        async with db.transaction():
            await db.execute(delete_user_leagues_query)
            print(f"Leagues for user: {user_id} cleaned.")

            # Prepare data tuple for insertion
            values = [
                (
                    session_id,
                    user_id,
                    user_name,
                    league[1],  # league_id
                    league[0],  # league_name
                    league[2],  # avatar
                    league[3],  # total_rosters
                    league[4],  # qb_cnt
                    league[5],  # rb_cnt
                    league[6],  # wr_cnt
                    league[7],  # te_cnt
                    league[8],  # flex_cnt
                    league[9],  # sf_cnt
                    league[10], # starter_cnt
                    league[11], # total_roster_cnt
                    league[12], # sport
                    entry_time,
                    league[13], # rf_cnt
                    league[14], # league_cat
                    league[15], # league_year
                    league[16]  # previous_league_id
                )
                for league in leagues
            ]

            # Insert data
            await db.executemany("""
                INSERT INTO dynastr.current_leagues (
                    session_id, user_id, user_name, league_id, league_name, avatar, 
                    total_rosters, qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt, 
                    starter_cnt, total_roster_cnt, sport, insert_date, rf_cnt, league_cat, 
                    league_year, previous_league_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                ON CONFLICT (session_id, league_id) DO UPDATE 
                SET
                    user_id = excluded.user_id,
                    user_name = excluded.user_name,
                    league_id = excluded.league_id,
                    league_name = excluded.league_name,
                    avatar = excluded.avatar,
                    total_rosters = excluded.total_rosters,
                    qb_cnt = excluded.qb_cnt,
                    rb_cnt = excluded.rb_cnt,
                    wr_cnt = excluded.wr_cnt,
                    te_cnt = excluded.te_cnt,
                    flex_cnt = excluded.flex_cnt,
                    sf_cnt = excluded.sf_cnt,
                    starter_cnt = excluded.starter_cnt,
                    total_roster_cnt = excluded.total_roster_cnt,
                    sport = excluded.sport,
                    insert_date = excluded.insert_date,
                    rf_cnt = excluded.rf_cnt,
                    league_cat = excluded.league_cat,
                    league_year = excluded.league_year,
                    previous_league_id = excluded.previous_league_id
                """, values)
    except Exception as e:
        print(f"Failed to update current leagues: {e}")
        traceback.print_exc() 
        raise  # Optionall

# def insert_league(db, league_data: LeagueDataModel):
#     print('executing')
#     user_id = "342397313982976000"
#     entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
#     session_id = 'cc73437c-40b6-4cd4-9835-6b41033968c4'
#     league_single = make_api_call(
#         f"https://api.sleeper.app/v1/league/{league_data.league_id}")
#     try:
#         qbs = len([i for i in league_single["roster_positions"] if i == "QB"])
#         rbs = len([i for i in league_single["roster_positions"] if i == "RB"])
#         wrs = len([i for i in league_single["roster_positions"] if i == "WR"])
#         tes = len([i for i in league_single["roster_positions"] if i == "TE"])
#         flexes = len(
#             [i for i in league_single["roster_positions"] if i == "FLEX"])
#         super_flexes = len(
#             [i for i in league_single["roster_positions"] if i == "SUPER_FLEX"]
#         )
#         rec_flexes = len(
#             [i for i in league_single["roster_positions"] if i == "REC_FLEX"]
#         )
#     except Exception as e:
#         print(f"An error occurred: {e} on league_single call, session_id: {session_id}, user_id: {user_id}, league_id:{league_data.league_id}")

#     starters = sum([qbs, rbs, wrs, tes, flexes, super_flexes, rec_flexes])
#     user_name = get_user_name(user_id)[1]
#     cursor = db.cursor()
#     # Execute an INSERT statement
#     cursor.execute(
#         """
#     INSERT INTO dynastr.current_leagues 
#     (session_id, user_id, user_name, league_id, league_name, avatar, total_rosters, qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt, starter_cnt, total_roster_cnt, sport, insert_date, rf_cnt, league_cat, league_year, previous_league_id, league_status) 
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (session_id, league_id) DO UPDATE 
#     SET user_id = excluded.user_id,
#   		user_name = excluded.user_name,
# 		league_id = excluded.league_id,
# 		league_name = excluded.league_name,
# 		avatar = excluded.avatar,
# 		total_rosters = excluded.total_rosters,
# 		qb_cnt = excluded.qb_cnt,
# 		rb_cnt = excluded.rb_cnt,
# 		wr_cnt = excluded.wr_cnt,
# 		te_cnt = excluded.te_cnt,
# 		flex_cnt = excluded.flex_cnt,
# 		sf_cnt = excluded.sf_cnt,
# 		starter_cnt = excluded.starter_cnt,
# 		total_roster_cnt = excluded.total_roster_cnt,
# 		sport = excluded.sport,
#       	insert_date = excluded.insert_date,
#         rf_cnt = excluded.rf_cnt,
#         league_cat = excluded.league_cat,
#         league_year = excluded.league_year,
#         previous_league_id = excluded.previous_league_id,
#         league_status = excluded.league_status;""",
#         (
#             session_id,
#             user_id,
#             user_name,
#             league_data.league_id,
#             league_single["name"],
#             league_single["avatar"],
#             league_single["total_rosters"],
#             qbs,
#             rbs,
#             wrs,
#             tes,
#             flexes,
#             super_flexes,
#             starters,
#             len(league_single["roster_positions"]),
#             league_single["sport"],
#             entry_time,
#             rec_flexes,
#             league_single["settings"]["type"],
#             league_single["season"],
#             league_single["previous_league_id"],
#             league_single["status"],
#         ),
#     )

#     # Commit the transaction
#     db.commit()

#     # Close the cursor and connection
#     cursor.close()

#     return


async def insert_managers(db, managers: list):
    # Prepare the SQL query and the data to be inserted
    sql = """
        INSERT INTO dynastr.managers (source, user_id, league_id, avatar, display_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id)
        DO UPDATE SET
            source = EXCLUDED.source,
            league_id = EXCLUDED.league_id,
            avatar = EXCLUDED.avatar,
            display_name = EXCLUDED.display_name;
    """
    # Create a list of tuples from the managers data
    values = [(manager[0], manager[1], manager[2], manager[3], manager[4]) for manager in iter(managers)]

    # Execute the batch operation
    async with db.transaction():
        await db.executemany(sql, values)
    return



async def insert_league_rosters(db, session_id: str, user_id: str, league_id: str) -> None:
    entry_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    rosters = await get_league_rosters(league_id)  # Ensure this is an async call

    league_players = []
    for roster in rosters:
        league_roster = roster["players"]
        try:
            for player_id in league_roster:
                league_players.append(
                    (session_id, user_id, player_id, roster["league_id"],
                     roster.get("owner_id", "EMPTY"), entry_time)
                )
        except KeyError:
            continue  # Skip any rosters that do not have the necessary data

    sql = """
        INSERT INTO dynastr.league_players 
        (session_id, owner_user_id, player_id, league_id, user_id, insert_date)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (session_id, user_id, player_id, league_id)
        DO UPDATE SET insert_date = EXCLUDED.insert_date;
    """
    # Execute the batch insertion using executemany
    async with db.transaction():
        await db.executemany(sql, league_players)
    return


async def total_owned_picks(
    db,
    league_id: str,
    session_id: str,
    startup: bool,
    base_picks: dict = None,
    traded_picks_all: dict = None
):
    if base_picks is None:
        base_picks = {}
    if traded_picks_all is None:
        traded_picks_all = {}
    
    if startup is not None:
        league_size =  await get_league_rosters_size(league_id)
        total_picks =  await get_traded_picks(league_id)
        draft_id =  await get_draft_id(league_id)

        years = (
            [str(int(draft_id["season"]) + i) for i in range(1, 4)]
            if draft_id["status"] == "complete"
            else [str(int(draft_id["season"]) + i) for i in range(0, 3)]
        )
        rd = min(int(draft_id["settings"]["rounds"]), 4)
        rounds = list(range(1, rd + 1))

        traded_picks = [
            [pick["season"], pick["round"], pick["roster_id"], pick["owner_id"]]
            for pick in total_picks
            if pick["roster_id"] != pick["owner_id"] and pick["season"] in years
        ]

        for year in years:
            base_picks[year] = {round_: [[i, i] for i in range(1, league_size + 1)] for round_ in rounds}
            for pick in traded_picks:
                traded_picks_all[year] = {
                    round_: [[i[2], i[3]] for i in traded_picks if i[0] == year and i[1] == round_]
                    for round_ in rounds
                }

        for year, traded_rounds in traded_picks_all.items():
            for round_, picks in traded_rounds.items():
                for pick in picks:
                    if [pick[0], pick[0]] in base_picks[year][round_]:
                        base_picks[year][round_].remove([pick[0], pick[0]])
                        base_picks[year][round_].append(pick)

        for year, rounds_ in base_picks.items():
            for round_, picks in rounds_.items():
                draft_picks = [
                    [year, str(round_), round_suffix(round_), str(pick[0]), str(pick[1]), str(league_id), draft_id["draft_id"], session_id]
                    for pick in picks
                ]

                # Execute the batch insertion using executemany
                sql = """
                    INSERT INTO dynastr.draft_picks (year, round, round_name, roster_id, owner_id, league_id, draft_id, session_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (year, round, roster_id, owner_id, league_id, session_id)
                    DO UPDATE SET round_name = EXCLUDED.round_name, draft_id = EXCLUDED.draft_id;
                """
                async with db.transaction():
                    await db.executemany(sql, draft_picks)
    return

async def draft_positions(db, league_id: str, user_id: str, draft_order: list = None) -> None:
    if draft_order is None:
        draft_order = []
    
    draft_id = await get_draft_id(league_id)
    draft =  await get_draft(draft_id["draft_id"])

    draft_dict = draft.get("draft_order", {})
    draft_slot = {k: v for k, v in draft["slot_to_roster_id"].items() if v is not None}
    season = draft["season"]
    rounds = min(int(draft_id["settings"]["rounds"]), 4)
    roster_slot = {int(k): v for k, v in draft_slot.items() if v is not None}
    rs_dict = dict(sorted(roster_slot.items(), key=lambda item: int(item[0])))

    if not draft_dict:
        participants = await get_roster_ids(league_id)
        for pos, (user_id, roster_id) in enumerate(participants):
            position_name = "Mid"
            draft_set = "N"
            draft_order.append([str(season), str(rounds), str(pos + 1), str(position_name), str(roster_id), str(user_id), str(league_id), str(draft_id["draft_id"]), str(draft_set)])
    else:
        league =  await get_league_rosters(league_id)
        empty_team_count = 0
        for k, v in draft_slot.items():
            if int(k) not in list(draft_dict.values()):
                owner_id = league[v - 1]["owner_id"]
                if owner_id:
                    draft_dict[owner_id] = int(k)
                else:
                    empty_alias = f"Empty_Team{empty_team_count}"
                    draft_dict[empty_alias] = v
                    empty_team_count += 1

        draft_order_dict = dict(sorted(draft_dict.items(), key=lambda item: item[1]))
        draft_order_ = {value: key for key, value in draft_order_dict.items()}

        for draft_position, roster_id in rs_dict.items():
            draft_set = "Y"
            position_name = "Early" if draft_position <= 4 else "Mid" if draft_position <= 8 else "Late"
            owner_id = draft_order_.get(int(draft_position), "Empty")
            draft_order.append([str(season), str(rounds), str(draft_position), str(position_name), str(roster_id), str(owner_id), str(league_id), str(draft_id["draft_id"]), str(draft_set)])

    # Execute the batch insertion asynchronously
    sql = """
        INSERT INTO dynastr.draft_positions (season, rounds, position, position_name, roster_id, user_id, league_id, draft_id, draft_set_flg)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (season, rounds, position, user_id, league_id)
        DO UPDATE SET position_name = EXCLUDED.position_name,
                      roster_id = EXCLUDED.roster_id,
                      draft_id = EXCLUDED.draft_id,
                      draft_set_flg = EXCLUDED.draft_set_flg;
    """
    async with db.transaction():
        await db.executemany(sql, draft_order)
    
    return


async def clean_player_trades(db, league_id: str) -> None:
    delete_query = """
        DELETE FROM dynastr.player_trades 
        WHERE league_id = $1;
    """
    # Execute the delete query asynchronously within a transaction
    async with db.transaction():
        await db.execute(delete_query, league_id)
    return


async def clean_draft_trades(db, league_id: str) -> None:
    delete_query = """
        DELETE FROM dynastr.draft_pick_trades 
        WHERE league_id = $1;
    """
    # Execute the delete query asynchronously within a transaction
    async with db.transaction():
        await db.execute(delete_query, league_id)
    return



async def get_trades(league_id: str, nfl_state: dict, year_entered: str) -> list:
    leg = max(nfl_state.get("leg", 1), 1)
    all_trades = []

    async def fetch_week_transactions(week):
        url = f"https://api.sleeper.app/v1/league/{league_id}/transactions/{week}"
        transactions = await make_api_call(url)
        all_trades.extend([t for t in transactions if t["type"] == "trade"])

    if nfl_state["season_type"] != "off":
        tasks = [fetch_week_transactions(week) for week in range(1, leg + 1)]
    elif year_entered != nfl_state["season"]:
        tasks = [fetch_week_transactions(week) for week in range(1, 18)]  # Assuming 17 weeks in the NFL season
    else:
        tasks = [fetch_week_transactions(1)]

    await asyncio.gather(*tasks)
    return all_trades



async def get_sleeper_state() -> str:
    try:
        url = "https://api.sleeper.app/v1/state/nfl"
        state = await make_api_call(url)
        return state
    except Exception as e:
        print(f"Error fetching NFL state from Sleeper API: {e}")
        raise  # Optionally, re-raise the exception or handle it more gracefully



async def insert_trades(db, trades: dict, league_id: str) -> None:
    player_adds_db = []
    player_drops_db = []
    draft_adds_db = []
    draft_drops_db = []

    for trade in trades:
       for roster_id in trade["roster_ids"]:
            player_adds = trade["adds"] if trade["adds"] else {}
            player_drops = trade["drops"] if trade["drops"] else {}
            draft_picks = trade["draft_picks"] if trade["draft_picks"] else [{}]

            for a_player_id, a_id in [
                [k, v] for k, v in player_adds.items() if v == roster_id
            ]:
                player_adds_db.append(
                    [
                        str(trade["transaction_id"]),
                        str(trade["status_updated"]),
                        str(a_id),
                        "add",
                        str(a_player_id),
                        str(league_id),
                    ]
                )
            for d_player_id, d_id in [
                [k, v] for k, v in player_drops.items() if v == roster_id
            ]:

                player_drops_db.append(
                    [
                        str(trade["transaction_id"]),
                        str(trade["status_updated"]),
                        str(d_id),
                        "drop",
                        str(d_player_id),
                        str(league_id),
                    ]
                )

            for pick in draft_picks:
                draft_picks_ = [v for k, v in pick.items()]

                if draft_picks_:
                    suffix = round_suffix(draft_picks_[1])
                    draft_adds_db.append(
                        [
                            str(trade["transaction_id"]),
                            str(trade["status_updated"]),
                            str(draft_picks_[4]),
                            "add",
                            str(draft_picks_[0]),
                            str(draft_picks_[1]),
                            str(suffix),
                            str(draft_picks_[2]),
                            str(league_id),
                        ]
                    )
                    draft_drops_db.append(
                        [
                            str(trade["transaction_id"]),
                            str(trade["status_updated"]),
                            draft_picks_[3],
                            "drop",
                            str(draft_picks_[0]),
                            str(draft_picks_[1]),
                            str(suffix),
                            str(draft_picks_[2]),
                            str(league_id),
                        ]
                    )

    draft_adds_db = dedupe(draft_adds_db)
    player_adds_db = dedupe(player_adds_db)
    player_drops_db = dedupe(player_drops_db)
    draft_drops_db = dedupe(draft_drops_db)

    # Use asyncpg's executemany for batch operations within a transaction
    async with db.transaction():
        draft_adds_query = """
            INSERT INTO dynastr.draft_pick_trades (transaction_id, status_updated, roster_id, transaction_type, season, round, round_suffix, org_owner_id, league_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING;
        """
        draft_drops_query = draft_adds_query  # Same structure
        player_adds_query = """
            INSERT INTO dynastr.player_trades (transaction_id, status_updated, roster_id, transaction_type, player_id, league_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING;
        """
        player_drops_query = player_adds_query  # Same structure

        await db.executemany(draft_adds_query, draft_adds_db)
        await db.executemany(draft_drops_query, draft_drops_db)
        await db.executemany(player_adds_query, player_adds_db)
        await db.executemany(player_drops_query, player_drops_db)

    return



async def player_manager_rosters(db, roster_data: RosterDataModel):
    session_id = roster_data.guid
    user_id = roster_data.user_id
    league_id = roster_data.league_id
    year_entered = roster_data.league_year
    startup = False

    try:
        # Perform cleaning operations
        print("performing roster cleaning operations")
        await clean_league_managers(db, league_id)
        await clean_league_rosters(db, session_id, league_id)
        await clean_league_picks(db, league_id, session_id)
        await clean_draft_positions(db, league_id)
    except Exception as e:
        print('issue1', e)
        return e
    try:
        print("fetching managers")
        # Fetch managers and insert them
        managers = await get_managers(league_id) 
        await insert_managers(db, managers) 
    except Exception as e:
        print('issue2', e)
        return e
    
        
    try:
        print("Inserting rosters and managing picks")
        # Insert rosters and manage picks
        await insert_league_rosters(db, session_id, user_id, league_id)
    except Exception as e:
        print('issue3', e)
        return e    
    
    print("Getting trades")
    await total_owned_picks(db, league_id, session_id, startup)
    await draft_positions(db, league_id, user_id)

   

    try:
        print("cleaning trades")
        # Handle trades
        await clean_player_trades(db, league_id)
        await clean_draft_trades(db, league_id)
    except Exception as e:
        print('issue4', e)
        return e
    try:
        # Get trades and insert them
        trades = await get_trades(league_id, await get_sleeper_state(), year_entered)
    except Exception as e:
        print('issue5', e)
        return e
    try:
        print("inserting Trades")
        await insert_trades(db, trades, league_id)
    except Exception as e:
        print(f"Issue: {e}")
        traceback.print_exc()  # This prints the stack trace to stdout
        return e
