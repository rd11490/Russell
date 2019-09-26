import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
from scipy.optimize import minimize, newton_krylov, broyden1
import datetime
import time
import numpy as np
from numba import jit, float64, int64
import operator
import multiprocessing as mp


# Simple_version

@jit
def denominator(prev_denom, days_rest, den_incr, beta, default_den):
    if prev_denom is None:
        return default_den
    else:
        return (prev_denom + den_incr) * (beta ** days_rest)


@jit
def numerator(prev_numer, days_rest, num_incr, beta, default_num):
    if prev_numer is None:
        return default_num
    else:
        return (prev_numer + num_incr) * (beta ** days_rest)


@jit
def decay_loop(mat, beta, default_den, default_num):
    new = []
    prev_denom = None
    prev_numer = None

    for i in range(0, mat.shape[0]):  # This is filling in the the information for the *previous game*

        den_incr = mat[i, 0]  # df_decay['denom_increment'].values[i]
        num_incr = mat[i, 1]  # df_decay['num_increment'].values[i]
        days_rest = mat[i, 2]  # df_decay['days_rest'].values[i]

        prev_denom = denominator(prev_denom, days_rest, den_incr, beta, default_den)
        prev_numer = numerator(prev_numer, days_rest, num_incr, beta, default_num)
        if prev_denom == 0:
            new.append(0)
        else:
            new.append(prev_numer / prev_denom)
    return new


def decay_method_rd(df_decay, beta, default_den, default_num):
    mat = df_decay[['denom_increment', 'num_increment', 'days_rest']].values

    new = decay_loop(mat, beta, default_den, default_num)

    df_decay['proj'] = new
    return df_decay["proj"]


def decay_solve(guesses, X, y, weight):
    if guesses[0] > 1:
        guesses[0] = 1
    proj = X.groupby('PLAYER_ID').apply(decay_method_rd, guesses[0], guesses[1], guesses[2])
    miss = y - proj.reset_index(level=0, drop=True)
    square_error = (miss ** 2)
    weighted_error = (square_error * X[weight]).sum(axis=0)
    # print(guesses, weighted_error)
    return weighted_error

def single_stat_solve(df, metric, tol, guesses):
    print("Solving Stat", metric)
    start = time.time()
    df_temp = df.copy()
    stat_to_solve = metric  # This the stat we're solving for, e.g., 3PT% or rebounds/48
    weight = weights[metric]  # This is the denominator (all the stats are rate stats), so like 3PA or minutes played.

    df_temp = format_dataframe(df_temp, metric, metric)

    if stat_to_solve == "PM_48":
        bnds = [(0.1, 1.0), (-5000, 5000), (-5000, 5000)]
    else:
        bnds = [(0.1, 1.0), (0.001, 5000), (0.001, 5000)]

    solutions = {}
    solutions_rmse = {}
    print("Trying LBFGSB")
    decay_solve_lmbda = lambda x: decay_solve(x, df_temp, df_temp[stat_to_solve], weight)
    decay_solution_LBFGSB = newton_krylov(decay_solve_lmbda, guesses, f_tol=tol, method='lgmres')
    # decay_solution_LBFGSB = minimize(decay_solve, guesses, args=(df_temp, df_temp[stat_to_solve], weight), bounds=bnds,
    #                                  method="L-BFGS-B", tol=tol)
    solutions_rmse["LBFGSB"] = decay_solution_LBFGSB.fun
    solutions["LBFGSB"] = decay_solution_LBFGSB

    winning_model = max(solutions_rmse.items(), key=operator.itemgetter(1))[0]

    print("Best model is : " + winning_model)

    decay_solution = solutions[winning_model]

    rmse = (decay_solution.fun / df_temp[weights[metric]].sum()) ** 0.5
    print("RMSE is " + to_str(rmse))

    beta = decay_solution.x[0]
    default_den = decay_solution.x[1]
    default_num = decay_solution.x[2]
    df_proj = df.copy()
    df_proj["denom_increment"] = df_temp["denom_increment"]
    df_proj["num_increment"] = df_temp["num_increment"]
    df_proj["x" + stat_to_solve] = df_proj.groupby('PLAYER_ID').apply(decay_method_rd, beta, default_den,
                                                                      default_num).reset_index(level=0, drop=True)

    regression_results = {}
    sol = [beta, default_den, default_num, decay_solution.fun, decay_solution.success, winning_model]
    regression_results[stat_to_solve] = sol
    regression_results_temp = pd.DataFrame.from_dict(regression_results, orient='index',
                                                     columns=['beta', 'regressWeight', 'regressValue', 'square_error',
                                                              'success', 'model'])

    return regression_results_temp, df_proj["x" + stat_to_solve]


def format_dataframe(df_toFormat, metric_to_format, stat_to_solve):
    weight = weights[
        metric_to_format]  # This is the denominator (all the stats are rate stats), so like 3PA or minutes played.
    df_toFormat["weighted_stat"] = (df_toFormat[stat_to_solve] * df_toFormat[
        weight])  # This gives the actual stat observed in non-rate terms, so 3PM, or rebounds.
    df_toFormat["denom_increment"] = df_toFormat.groupby('PLAYER_ID')[weight].shift(
        1)  # This tells you what the denominator was yesterday, so the predict is OOS.
    df_toFormat["num_increment"] = df_toFormat.groupby('PLAYER_ID')["weighted_stat"].shift(
        1)  # This tells you what the numerator was yesterday (e.g., the 3PM or rebounds)

    return df_toFormat





def to_str(var):
    if type(var) is list:
        return str(var)[1:-1]  # list
    if type(var) is np.ndarray:
        try:
            return str(list(var[0]))[1:-1]  # numpy 1D array
        except TypeError:
            return str(list(var))[1:-1]  # numpy sequence
    return str(var)  # everything else




metrics = ['FGA_48',
           'FG_PCT',
           'FG2A_48',
           'FG2_PCT',
           'FG3A_48',
           'FG3_PCT',
           'FTA_48',
           'FT_PCT',
           'ORB_48',
           'DRB_48',
           'TRB_48',
           'AST_48',
           'STL_48',
           'BLK_48',
           'TO_48',
           'PF_48',
           'PM_48']

if __name__ == '__main__':
    # Read in The Minutes Projections
    df = pd.read_csv("test_gl.csv")

    # Drop NaN
    df.dropna(subset=['FGM', 'FGA'], thresh=2, inplace=True)

    # Sort

    df.sort_values(by=['PLAYER_ID', 'date'], inplace=True)

    # Cap the days_rest part.

    df["days_rest"] = np.where(df['days_rest'] > 200, 200, df['days_rest'])

    # Sets a multiplier so everything ends up per 48. Means you need to multiply the numerator by 48.

    min_adj = 48

    df.head()

    weights = {'FGA_48': 'orig_minutes',
               'FG_PCT': 'orig_FGA',
               'FG2A_48': 'orig_minutes',
               'FG2_PCT': 'orig_FG2A',
               'FG3A_48': 'orig_minutes',
               'FG3_PCT': 'orig_FG3A',
               'FTA_48': 'orig_minutes',
               'FT_PCT': 'orig_FTA',
               'ORB_48': 'orig_minutes',
               'DRB_48': 'orig_minutes',
               'TRB_48': 'orig_minutes',
               'AST_48': 'orig_minutes',
               'STL_48': 'orig_minutes',
               'BLK_48': 'orig_minutes',
               'TO_48': 'orig_minutes',
               'PF_48': 'orig_minutes',
               'PM_48': 'orig_minutes',
               'adj_minutes': 'gp_dummy'}



    print('Cores available: ',mp.cpu_count())

    pool = mp.Pool(mp.cpu_count())
    tol = 0.001
    guesses = [0.99, 738.6109906, -2510.81993779696]

    results = []
    for metric in metrics:
        out = pool.apply_async(single_stat_solve, args=(df, metric, tol, guesses))
        results.append(out)

    for r in results:
        print(r)

    pool.close()
    pool.join()

    print('Getting Results')
    for r in results:
        print(r.get())



    # guesses = [0.99, 738.6109906, -2510.81993779696]
    # weight = weights[stat_to_solve] # This is the denominator (all the stats are rate stats), so like 3PA or minutes played.
    # tol = 0.001

    # single_stat_solve_lmbda=lambda metric:single_stat_solve(df,metric,tol,guesses)

    # print('Running in ||')
    # dview.map_sync(single_stat_solve_lmbda, metrics)