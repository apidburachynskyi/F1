#%%

import pandas as pd
import numpy as np

#Load & change variable name (to match model prod)

df = pd.read_csv("Data/race_results_2000_2024.csv")

df['event'] = df['circuitId']
df['year'] = df['season']
df['FullName'] = df['driverName']
df['TeamName'] = df['constructorName']
df['EventDate'] = pd.to_datetime(df['date'])
df['Position'] = df['position']
df['GridPosition'] = df['grid'].replace(0, 21)  # Pit lane = 21

df.sort_values(by=["FullName", "EventDate"], inplace=True)

#%%

# Team Heritage

TEAM_Heritage = {
    'Toro Rosso': 'RB_Junior_Team',
    'AlphaTauri': 'RB_Junior_Team',
    'RB F1 Team': 'RB_Junior_Team',
    'Renault': 'Alpine_Family',
    'Alpine F1 Team': 'Alpine_Family',
    'Force India': 'Racing_Point_Family',
    'Racing Point': 'Racing_Point_Family',
    'Aston Martin': 'Racing_Point_Family',
    'Sauber': 'Alfa_Romeo_Family',
    'Alfa Romeo': 'Alfa_Romeo_Family',
}
df['TeamFamily'] = df['TeamName'].map(TEAM_Heritage).fillna(df['TeamName'])

#  Feature Creation (some useless)

def count_agg(count_col: str, groupby_col: list, df: pd.DataFrame):
    group_name = '_and_'.join(groupby_col)
    col = f'count_{count_col}_by_{group_name}'
    df[col] = df.groupby(groupby_col)[count_col].transform(lambda x: x.shift(1).expanding().count())
    return df

def agg(agg_col: str, groupby_col: list, df: pd.DataFrame, agg_calc: str, s: int, r: int):
    group_name = '_and_'.join(groupby_col)
    if s < 100:
        col = f'{agg_calc}_{agg_col}_by_{group_name}_previous_{s}_observations'
    else:
        col = f'{agg_calc}_{agg_col}_by_{group_name}'

    grp = df.groupby(groupby_col)[agg_col]

    if agg_calc == 'mean' and s > 1000:
        df[col] = grp.transform(lambda x: x.shift(1).expanding().mean())
    elif agg_calc == 'sum' and s > 1000:
        df[col] = grp.transform(lambda x: x.shift(1).expanding().sum())
    elif agg_calc == 'mean':
        df[col] = grp.transform(lambda x: x.shift(1).rolling(s, r).mean())
    elif agg_calc == 'sum':
        df[col] = grp.transform(lambda x: x.shift(1).rolling(s, r).sum())
    elif agg_calc == 'sumlast':
        col = f'last_{group_name}_{agg_col}'
        df[col] = grp.transform(lambda x: x.shift(1).sum())
    elif agg_calc == 'max':
        col = f'max_{group_name}_{agg_col}'
        df[col] = grp.transform(lambda x: x.max())

    return df

# Feature (shortned list)

# Experience
df = count_agg('EventDate', ['FullName'], df)

# Driver stats ongoing yeqr
df = agg('Position', ['FullName'], df, 'mean', 3, 1)
df = agg('GridPosition', ['FullName'], df, 'mean', 3, 1)

# Team stats ongoing yeqr
df = agg('Position', ['TeamFamily'], df, 'mean', 3, 1)
df = agg('GridPosition', ['TeamFamily'], df, 'mean', 3, 1)

# Team average performance last yeqr
df = agg('Position', ['TeamFamily', 'year'], df, 'mean', 100000, 1)
df = agg('GridPosition', ['TeamFamily', 'year'], df, 'mean', 100000, 1)

# Last yeqr championship
df = agg('points', ['year', 'FullName'], df, 'sumlast', 1, 1)
df = agg('points', ['year', 'TeamFamily'], df, 'sumlast', 1, 1)
df = agg('last_year_and_TeamFamily_points', ['year'], df, 'max', 100000, 1)
df = agg('last_year_and_FullName_points', ['year'], df, 'max', 100000, 1)

df['last_year_constructors_champ'] = (
    (df['last_year_and_TeamFamily_points'] == df['max_year_last_year_and_TeamFamily_points']) &
    (df['max_year_last_year_and_TeamFamily_points'] != 0)
)
df['last_year_drivers_champ'] = (
    (df['last_year_and_FullName_points'] == df['max_year_last_year_and_FullName_points']) &
    (df['max_year_last_year_and_FullName_points'] != 0)
)

#Short list version Features (add more from main if time, high impact reduce grid importance : track perfomance info, car relabiliity, overall car/driver performance evolution throught the years)

final_features = [
    "Position",                                # Target(sort of)
    "GridPosition",                            # Starting grid position
    "event",                                   # Circuit
    "year",                                    # Season
    "TeamFamily",                              # Heritage team grouping
    "FullName",                                # Driver name
    # Breakthrought potential
    "mean_Position_by_FullName_previous_3_observations",      # Driver form (average position last 3 races)
    "mean_GridPosition_by_FullName_previous_3_observations",  # Driver quali form (average position on the grid last 3 races)
 
    "mean_Position_by_TeamFamily_previous_3_observations",     # Team recent performance
    "mean_GridPosition_by_TeamFamily_previous_3_observations", # Team recent qualifying

    "mean_Position_by_TeamFamily_and_year",                   # Team avg finish last yeqr
    "mean_GridPosition_by_TeamFamily_and_year",               # Team avg grid last yeqr

    "last_year_drivers_champ",                  # Prior driver champ flag (previous yeqr driver champ)
    "last_year_constructors_champ",             # Prior team champ flag (previous yeqr contractor champ)

    "count_EventDate_by_FullName",              # Driver experience (number of races driven)
]


final_df = df[final_features]
final_df = final_df[final_df["year"] >= 2015].reset_index(drop=True)
final_df.to_csv("Data/race_feature_2015_2024.csv", index=False)

# %%
