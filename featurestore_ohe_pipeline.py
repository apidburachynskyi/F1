import pandas as pd

from timple.timedelta import strftimedelta
import requests
import timeit

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
race_df = pd.read_csv('csv/race_silver_df.csv')


def feat_ohe(uniqueid_list:list, col_list:list, df:pd.DataFrame):

    df_ohe = pd.get_dummies(df[col_list],dtype=float)

    df_unique = df[uniqueid_list]

    df = pd.concat([df_unique, df_ohe], axis = 1)

    return df

race_df = feat_ohe(['year','event','FullName'],['event','TeamName','Status','FullName'], race_df)
race_df.to_csv('csv/race_feature_ohe.csv')
