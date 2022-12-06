import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

race_df = pd.read_csv('csv/race_silver_df.csv')
quali_df = pd.read_csv('csv/quali_silver_df.csv')
feat_race_agg = pd.read_csv('csv/race_feature_agg.csv')
feat_race_ohe = pd.read_csv('csv/race_feature_ohe.csv')

quali_df.drop(feat_race_agg.columns[0],axis=1,inplace=True)
race_df.drop(race_df.columns[[0,1]],axis=1,inplace=True)
feat_race_agg.drop(feat_race_agg.columns[0],axis=1,inplace=True)
feat_race_ohe.drop(feat_race_ohe.columns[0],axis=1,inplace=True)

#print(quali_df.columns)

def join(df1:pd.DataFrame,df2:pd.DataFrame, join_cols:list):

    df = df1.merge(df2, how='left',on=join_cols)

    return df

race_df = join(race_df,feat_race_agg,['year','event','FullName'])
race_df = join(race_df,feat_race_ohe,['year','event','FullName'])
race_df = join(race_df,quali_df,['year','event','FullName'])

for col in quali_df.columns:

    print(col,":     ",quali_df[col].isna().sum())

for col in race_df.columns:

    print(col,":     ",race_df[col].isna().sum())

#print(quali_df.columns)

race_df['front_row'] = np.where(((race_df['GridPosition'] == 1) | (race_df['GridPosition'] == 2)), True, False)

race_df['isfirst'] = np.where(race_df['Position'] == 1, True, False)

race_df['istoptwo'] = np.where(((race_df['Position'] == 1) | (race_df['Position'] == 2)), True, False)

race_df['ispodium'] = np.where(((race_df['Position'] == 1) | (race_df['Position'] == 2) | (race_df['Position'] == 3)), True, False)

race_df['GridPosition'] = np.where(race_df['GridPosition'] == 0, 21, race_df['GridPosition'])

print(race_df.columns)
race_df.to_csv('csv/race_gold_df.csv')

