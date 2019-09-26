#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
from scipy.optimize import minimize
import datetime
import time
import feather
import numpy as np


# In[2]:


from pathlib import Path
base = Path.cwd()
raw = Path.cwd() / "raw"
reports = Path.cwd() / "reports"
interim = Path.cwd() / "interim"
p = Path(base)
parent = p.parent
path = raw / "pbp2019.feather"


# In[3]:


# Read in The Minutes Projections
try:
    df = feather.read_dataframe(raw / "pbp2019.feather")
except:
    df=pd.read_excel(raw / "pbp2019.xlsx", sheet_name="Sheet1", parse_dates=['date'])
    feather.write_dataframe(df, raw / "pbp2019.feather")

# Drop NaN
df.dropna(subset=['FGM', 'FGA'],thresh=2,inplace=True)


# In[4]:


# Sort and calculate date of last game

df.sort_values(by=['PLAYER_ID','date'], inplace=True)
df["last_game"]=df.groupby('PLAYER_ID')['date'].shift(1)
df['days_rest'] = (df['date'] - df['last_game']).dt.days.fillna(0) # Calculate days since last game

# Create a games played dummy

df["gp_dummy"] = 1.0

df["B2B"] = np.where(df['days_rest'] ==1.0,1.0, 0.0)

df.head()


# In[5]:


# Create two point attempts.

df["FG2A"] = df.FGA-df.FG3A
df["FG2M"] = df.FGM-df.FG3M
df["FG2_PCT"] = df.FG2M/df.FG2A
df["FG2_PCT"] = df["FG2_PCT"].fillna(0)


# In[6]:


# Create new per 48 stats

df["FGA_48"] = df.FGA/df.minutes*48
df["FG2A_48"] = df.FG2A/df.minutes*48
df["FG3A_48"] = df.FG3A/df.minutes*48
df["FTA_48"] = df.FTA/df.minutes*48
df["ORB_48"] = df.OREB/df.minutes*48
df["DRB_48"] = df.DREB/df.minutes*48
df["TRB_48"] = df.REB/df.minutes*48
df["AST_48"] = df.AST/df.minutes*48
df["STL_48"] = df.STL/df.minutes*48
df["BLK_48"] = df.BLK/df.minutes*48
df["TO_48"] = df.TO/df.minutes*48
df["PF_48"] = df.PF/df.minutes*48
df["PM_48"] = df.PLUS_MINUS/df.minutes*48


# In[7]:


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
           'PM_48',
           'minutes']

weights = {'FGA_48':'minutes',
           'FG_PCT':'FGA',
           'FG2A_48':'minutes',
           'FG2_PCT': 'FG2A',
           'FG3A_48':'minutes',
           'FG3_PCT': 'FG3A',
           'FTA_48':'minutes',
           'FT_PCT':'FTA',
           'ORB_48':'minutes',
           'DRB_48':'minutes',
           'TRB_48':'minutes',
           'AST_48':'minutes',
           'STL_48':'minutes',
           'BLK_48':'minutes',
           'TO_48':'minutes',
           'PF_48':'minutes',
           'PM_48':'minutes',
           'minutes': 'gp_dummy'}


# In[8]:


def denominator(prev_denom, days_rest, den_incr, beta, default_den):
    if prev_denom is None:
        return default_den
    else:
        return (prev_denom+den_incr)*(beta**days_rest)
    
def numerator(prev_numer, days_rest, num_incr, context_val, beta, default_num):
    if prev_numer is None:
        return default_num
    else:
        return (prev_numer+num_incr-context_val)*(beta**days_rest)

def decay_method_rd(df_decay, beta, default_den, default_num, b2b_coef):
    new = []
    prev_denom = None
    prev_numer = None
    prev_b2b = 0
    for i in range(0, len(df_decay.index)): # This is filling in the the information for the *previous game*
        
        den_incr = df_decay['denom_increment'].values[i]
        num_incr = df_decay['num_increment'].values[i]
        days_rest = df_decay['days_rest'].values[i]
        
        context_val = prev_b2b*den_incr*b2b_coef
        # print(context_val)
                
        prev_b2b = df_decay['B2B'].values[i]     
        prev_denom = denominator(prev_denom, days_rest, den_incr, beta, default_den)
        prev_numer = numerator(prev_numer, days_rest, num_incr, context_val, beta, default_num)
        if prev_denom == 0:
            new.append(0)
        else:
            new.append(prev_numer/prev_denom + prev_b2b*b2b_coef)
    df_decay['proj'] = new
    return df_decay["proj"]

def decay_solve(guesses, X, y):
    proj = X.groupby('PLAYER_ID').apply(decay_method_rd, guesses[0],guesses[1],guesses[2],guesses[3])
    miss = y - proj.reset_index(level=0, drop=True)
    square_error = (miss**2)
    weighted_error = (square_error*X[weight]).sum(axis=0)
    print(guesses,weighted_error)
    return weighted_error


# In[9]:


def denominator(prev_denom, days_rest, den_incr, beta, default_den):
    if prev_denom is None:
        return default_den
    else:
        return (prev_denom+den_incr)*(beta**days_rest)
    
def numerator(prev_numer, days_rest, num_incr, context_val, beta, default_num):
    if prev_numer is None:
        return default_num
    else:
        return (prev_numer+num_incr-context_val)*(beta**days_rest)

def decay_method_rd(df_decay, beta, default_den, default_num, b2b_coef):
    new = []
    prev_denom = None
    prev_numer = None
    prev_b2b = 0
    for i in range(0, len(df_decay.index)): # This is filling in the the information for the *previous game*
        
        den_incr = df_decay['denom_increment'].values[i]
        num_incr = df_decay['num_increment'].values[i]
        days_rest = df_decay['days_rest'].values[i]
        
        context_val = prev_b2b*den_incr*b2b_coef
        # print(context_val)
                
        prev_b2b = df_decay['B2B'].values[i]     
        prev_denom = denominator(prev_denom, days_rest, den_incr, beta, default_den)
        prev_numer = numerator(prev_numer, days_rest, num_incr, context_val, beta, default_num)
        if prev_denom == 0:
            new.append(0)
        else:
            new.append(prev_numer/prev_denom + prev_b2b*b2b_coef)
    df_decay['proj'] = new
    return df_decay["proj"]

def decay_solve(guesses, X, y):
    proj = X.groupby('PLAYER_ID').apply(decay_method_rd, guesses[0],guesses[1],guesses[2],guesses[3])
    miss = y - proj.reset_index(level=0, drop=True)
    square_error = (miss**2)
    weighted_error = (square_error*X[weight]).sum(axis=0)
    print(guesses,weighted_error)
    return weighted_error

stat_to_solve = "STL_48"

def format_dataframe(df_toFormat,metric_to_format):

    weight = weights[metric_to_format] # This is the denominator (all the stats are rate stats), so like 3PA or minutes played.
    df_toFormat["weighted_stat"] = df_toFormat[stat_to_solve]*df_toFormat[weight] # This gives the actual stat observed in non-rate terms, so 3PM, or rebounds.
    df_toFormat["denom_increment"] = df_toFormat.groupby('PLAYER_ID')[weight].shift(1) # This tells you what the denominator was yesterday, so the predict is OOS.
    df_toFormat["num_increment"]=df_toFormat.groupby('PLAYER_ID')["weighted_stat"].shift(1) # This tells you what the numerator was yesterday (e.g., the 3PM or rebounds)
    
    return df_toFormat

df_temp = df.copy()
df_temp = format_dataframe(df_temp,stat_to_solve)

result = df_temp.head(20).groupby('PLAYER_ID').apply(decay_method_rd, 0.9, 330, 473, -0.06).reset_index(level=0, drop=True)

result

# Row 4 should be 1.73942974


# In[ ]:


# Solve a single stat
metrics = ['STL_48']

# Do a full Kostya Loop

regression_results_kostya = pd.DataFrame()

# Full loop

df_proj = df.copy()

start = time.time()
for metric in metrics:
    print (metric)
    df_temp = df.copy()
    
    stat_to_solve = metric # This the stat we're solving for, e.g., 3PT% or rebounds/48
    weight = weights[stat_to_solve] # This is the denominator (all the stats are rate stats), so like 3PA or minutes played.
    df_temp["weighted_stat"] = df_temp[stat_to_solve]*df_temp[weight] # This gives the actual stat observed in non-rate terms, so 3PM, or rebounds.
    df_temp["denom_increment"] = df_temp.groupby('PLAYER_ID')[weight].shift(1) # This tells you what the denominator was yesterday, so the predict is OOS.
    df_temp["num_increment"]=df_temp.groupby('PLAYER_ID')["weighted_stat"].shift(1) # This tells you what the numerator was yesterday (e.g., the 3PM or rebounds)

    if stat_to_solve == "PM_48":
        bnds =  [(0.000000001, 1.0),(-500, 500),(-500, 500),(-500, 500)]
    else:
        bnds =  [(0.000000001, 1.0),(0.001, 5000),(0.001, 5000),(-500, 500)]
    
    guesses = [0.99,200,55,0] # Decay, Denominator, Numerator, B2B value
    
    print ("Trying L_BFGF_B")
    decay_solution_L_BFGF_B = minimize(decay_solve, guesses, args=(df_temp, df_temp[stat_to_solve]), method = "L-BFGS-B",  bounds=bnds)
    print ("Trying SLSQP")
    decay_solution_SLSQP = minimize(decay_solve, guesses, args=(df_temp, df_temp[stat_to_solve]), method = "SLSQP",  bounds=bnds)

    # Create decay projections
    
    if decay_solution_L_BFGF_B.fun > decay_solution_SLSQP.fun:
        decay_solution = decay_solution_SLSQP
        print ("SLSQP Better")
    else:
        decay_solution = decay_solution_L_BFGF_B
        print ("L-BFG-B Better")
    
    print(decay_solution)
    
    beta = decay_solution.x[0]
    default_den = decay_solution.x[1]
    default_num = decay_solution.x[2]
    b2b_coef = decay_solution.x[3]
    
    df_proj["denom_increment"] = df_temp["denom_increment"]
    df_proj["num_increment"] = df_temp["num_increment"]
    df_proj["x" + stat_to_solve] = df_proj.groupby('PLAYER_ID').apply(decay_method_rd, beta, default_den, default_num, b2b_coef).reset_index(level=0, drop=True)
    
    regression_results = {}
    sol = [beta,default_den,default_num, b2b_coef, decay_solution.fun,decay_solution.success]
    regression_results[stat_to_solve]=sol
    regression_results_temp = pd.DataFrame.from_dict(regression_results,orient='index', columns=['beta', 'regressWeight', 'regressValue', 'b2b_value', 'square_error','success'])
    regression_results_kostya = regression_results_kostya.append(regression_results_temp) 

end = time.time()
print(start-end)


# In[ ]:


regression_results_kostya


# In[ ]:


feather.write_dataframe(df_proj, interim / "df_proj_kostya.feather")
regression_results_kostya.to_csv("kostya.csv")
regression_results_kostya


# In[ ]:


# PTS-1.2*TOV+0.7*BLK+1.5*STL+0.5*AST+0.2*DRB+0.3*ORB-0.3*FTA- 2PA - 0.8*3PA + GamesStarted% x 2.2 - 7.9

# https://bballhistory.wordpress.com/statistical-plusminus/

# https://fansided.com/2017/04/10/updating-dre-tweaks/


# In[ ]:


df_proj.head(10)


# In[ ]:




