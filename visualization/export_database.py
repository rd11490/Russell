import os

import pandas

import MySqlDatabases.NBADatabase as nba
from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()


tables = [
    # nba.game_officials,
    # nba.game_record,
    # nba.raw_game_inactive_players,
    # nba.raw_game_info,
    # nba.raw_game_other_stats,
    # nba.raw_game_score_line,
    # nba.raw_game_summary,
    # nba.raw_play_by_play,
    # nba.raw_shot_data,
    # nba.raw_player_profile_season_totals,
    # nba.roster_player,
    # nba.roster_coach,
    # nba.raw_team_box_score_advanced,
    # nba.raw_player_box_score_advanced,
    # nba.players_on_court,
    # nba.players_on_court_at_period,
    # nba.lineup_shots,
    # nba.team_info,
    # nba.play_by_play_with_lineup,
    # nba.team_scored_shots,
    # nba.offense_expected_points,
    # nba.defense_expected_points,
    # nba.offense_expected_points_by_player,
    # nba.defense_expected_points_by_player,
    # nba.offense_expected_points_by_game,
    # nba.defense_expected_points_by_game,
    # nba.shot_stint_data,
    # nba.luck_adjusted_stints,
    # nba.luck_adjusted_one_way_stints,
    # nba.luck_adjusted_units,
    # nba.real_adjusted_four_factors,
    # nba.real_adjusted_four_factors_multi,
    # nba.player_info,
    # nba.player_shot_charts,
    # nba.raw_player_profile_career_totals,
    # nba.luck_adjusted_possessions,
    nba.luck_adjusted_one_way_possessions
]

def build_path_for_dir(table):
    return "/Users/ryandavis/Documents/Workspace/NBAData/csv/{}".format(table)

def build_path_for_file(table, season):
    return "/Users/ryandavis/Documents/Workspace/NBAData/csv/{0}/{0}_{1}.csv".format(table, season)

def get_seasons_query(table):
    return "SELECT DISTINCT season FROM nba.{}".format(table)

def get_seasons(table):
    try:
        return list(sql.runQuery(get_seasons_query(table))["season"])
    except pandas.io.sql.DatabaseError as e:
        return[]

def select_single_season_query(table, season):
    return "SELECT * FROM nba.{0} WHERE season = '{1}'".format(table, season)

def select_table_query(table):
    return "SELECT * FROM nba.{0}".format(table)

def create_folder(table):
    if not os.path.exists(build_path_for_dir(table)):
        os.mkdir(build_path_for_dir(table))

for table in tables:
    print(table)
    seasons = get_seasons(table)
    create_folder(table)
    if (len(seasons) > 0):
        for season in seasons:
            print(season)
            results = sql.runQuery(select_single_season_query(table, season))
            results.to_csv(build_path_for_file(table, season))
    else:
        results = sql.runQuery(select_table_query(table))
        results.to_csv(build_path_for_file(table, "full"))



