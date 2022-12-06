import pandas as pd
from featurestore_ohe_pipeline import feat_ohe
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
race_df = pd.read_csv('csv/race_silver_df.csv')

def count_agg(count_col:str,groupby_col:list, df:pd.DataFrame):

    group_name = '_and_'.join(groupby_col)
    col = f'count_{count_col}_by_{group_name}'
    df[col] = df.groupby(groupby_col)[count_col].apply(lambda x: x.shift(1).expanding().count())

    return df

def agg(agg_col:str,groupby_col:list, df:pd.DataFrame, agg_calc:str, s:int, r:int):

    group_name = '_and_'.join(groupby_col)
    col = f'{agg_calc}_{agg_col}_by_{group_name}'

    if s < 100:
        col = f'{agg_calc}_{agg_col}_by_{group_name}_previous_{s}_observations'

    grp = df.groupby(groupby_col)[agg_col]

    if s > 1000 and agg_calc == 'mean':
        df[col] = (grp.apply(lambda x: x.shift(1).cumsum())) / grp.cumcount()
    elif s > 1000 and agg_calc == 'sum':
        df[col] = (grp.apply(lambda x: x.shift(1).cumsum()))
    elif agg_calc == 'mean':
        df[col] = df.groupby(groupby_col)[agg_col].transform(lambda x: x.shift(1).rolling(s, r).mean())
    elif agg_calc == 'sum':
        df[col] = df.groupby(groupby_col)[agg_col].transform(lambda x: x.shift(1).rolling(s, r).sum())
    elif agg_calc == 'sumlast':
        short_df = (grp.apply(lambda x: x.shift(1).sum())).reset_index()
        short_df['year'] = short_df['year'] + 1
        short_df.rename(columns = {agg_col:f'last_{group_name}_{agg_col}'}, inplace=True)
        df = df.merge(short_df, on=groupby_col, how='left').fillna(0)

    elif agg_calc == 'mean_team':
        short_df = (grp.apply(lambda x: x.mean())).reset_index()

        short_df.rename(columns = {agg_col:f'{agg_calc}_{agg_col}'}, inplace=True)

        df = df.merge(short_df, on=groupby_col, how='left')
    elif agg_calc == 'max':
        short_df = (grp.apply(lambda x: x.max())).reset_index()

        short_df.rename(columns = {agg_col:f'{agg_calc}_{agg_col}'}, inplace=True)

        df = df.merge(short_df, on=groupby_col, how='left')

    return df

def race_agg(df:pd.DataFrame):

    #Select relevent columns
    df = df[['TeamName','FullName','Position','GridPosition','Points','year','event','Status','EventDate']]
    print(df.shape)

    #create positions gained column for aggregation
    df['positions_gained'] = df['GridPosition'] - df['Position']

    #Create one_hot_encoded columns for status aggregation
    status_ohe = feat_ohe(['year','event','FullName'],['Status'], df)

    #Merge back to ohe
    df = df.merge(status_ohe, how='left', on =['year','event','FullName'])


   ###TOTAL RACE STATS

    #Total Races for a Driver per each Track
    df = count_agg('EventDate',['FullName','event'],df)

    #Total Races for a Driver
    df = count_agg('EventDate',['FullName'],df)

    #Total Races for a Team
    df = count_agg('EventDate',['TeamName'],df)


    ###LAST YEAR STATS

    #Total Points for a Driver Last year
    df = agg('Points',['year','FullName'],df,'sumlast',1,1)

    #Total Points for a Team Last year
    df = agg('Points',['year','TeamName'],df,'sumlast',1,1)

    ####TRACK STATS

    #Average number of accidents per track
    df = agg('Status_Driver_Error',['event'],df,'mean',100000,1)

    #Average number of car failures per track
    df = agg('Status_Car_Failure',['event'],df,'mean',100000,1)




    #Average finishing position per grid position per track
    #df = agg('Position',['event','GridPosition'],df,'mean',100000,1)

    ####AVERAGE DRIVER / TEAM DNFs

    #Average number of accidents per driver
    df = agg('Status_Driver_Error',['FullName'],df,'mean',100000,1)

    #Average car failures per team
    df = agg('Status_Car_Failure',['TeamName'],df,'mean',100000,1)


    ####AVERAGE / LAST DRIVER FINISHING POSITIONS

    #Last finishing position per driver
    df = agg('Position',['FullName'],df,'mean',1,1)

    #Last finishing position per driver and track
    df = agg('Position',['FullName','event'],df,'mean',1,1)

    #Average finishing position per driver and track
    df = agg('Position',['FullName','event'],df,'mean',100000,1)

    #Average finishing position per driver and track for last 3 races
    df = agg('Position',['FullName','event'],df,'mean',3,1)

    #Average finishing position per driver in the last 10 races
    df = agg('Position',['FullName'],df,'mean',10,1)

    #Average finishing position per driver for that year
    df = agg('Position',['FullName','year'],df,'mean',100000,1)

    #Average finishing position per driver overall
    df = agg('Position',['FullName'],df,'mean',10000,1)



    ####AVERAGE / LAST TEAM FINISHING POSITIONS
    df = agg('Position',['TeamName','event','year'],df,'mean_team',1,1)


    #Last finishing position per team
    df = agg('mean_team_Position',['FullName'],df,'mean',1,1)

    #Last finishing position per team and track
    df = agg('mean_team_Position',['FullName','event'],df,'mean',1,1)

    #Average finishing position per team and track
    df = agg('mean_team_Position',['FullName','event'],df,'mean',100000,1)

    #Average finishing position per team and track for last 3 races
    df = agg('mean_team_Position',['FullName','event'],df,'mean',3,1)

    #Average finishing position per team in the last 10 races
    df = agg('mean_team_Position',['FullName'],df,'mean',10,1)

    #Average finishing position per team for that year
    df = agg('mean_team_Position',['FullName','year'],df,'mean',100000,1)

    #Average finishing position per team overall
    df = agg('mean_team_Position',['FullName'],df,'mean',10000,1)




    ####AVERAGE / LAST DRIVER QUALIFYING POSITIONS

    #Last GridPosition position per driver
    df = agg('GridPosition',['FullName'],df,'mean',1,1)

    #Last GridPosition per driver and track
    df = agg('GridPosition',['FullName','event'],df,'mean',1,1)

    #Average GridPosition per driver and track
    df = agg('GridPosition',['FullName','event'],df,'mean',100000,1)

    #Average GridPosition per driver and track for last 3 races
    df = agg('GridPosition',['FullName','event'],df,'mean',3,1)

    #Average GridPosition per driver in the last 10 races
    df = agg('GridPosition',['FullName'],df,'mean',10,1)

    #Average GridPosition per driver for that year
    df = agg('GridPosition',['FullName','year'],df,'mean',100000,1)

    #Average GridPosition per driver overall
    df = agg('GridPosition',['FullName'],df,'mean',10000,1)



    ####AVERAGE / LAST TEAM QUALIFYING POSITIONS
    df = agg('GridPosition',['TeamName','event','year'],df,'mean_team',1,1)

    #Last GridPosition per team
    df = agg('mean_team_GridPosition',['FullName'],df,'mean',1,1)

    #Last GridPosition per team and track
    df = agg('mean_team_GridPosition',['FullName','event'],df,'mean',1,1)

    #Average GridPosition per team and track
    df = agg('mean_team_GridPosition',['FullName','event'],df,'mean',100000,1)

    #Average GridPosition per team and track for last 3 races
    df = agg('mean_team_GridPosition',['FullName','event'],df,'mean',3,1)

    #Average GridPosition per team in the last 10 races
    df = agg('mean_team_GridPosition',['FullName'],df,'mean',10,1)

    #Average GridPosition per team for that year
    df = agg('mean_team_GridPosition',['FullName','year'],df,'mean',100000,1)

    #Average GridPosition per team overall
    df = agg('mean_team_GridPosition',['FullName'],df,'mean',10000,1)








    #Average qualification result per driver and track
    #df = agg('GridPosition',['FullName','event'],df,'mean',100000,1)

    #Average positions gained per driver and track
    df = agg('positions_gained',['FullName','event'],df,'mean',100000,1)

    #Driver championship points in current season
    df = agg('Points',['FullName','year'],df,'sum',100000,1)

    #Constructors Championship points in current season
    df = agg('Points',['TeamName','year'],df,'sum',100000,1)

    df = agg('last_year_and_FullName_Points',['year'],df,'max',100000,1)

    df = agg('last_year_and_TeamName_Points',['year'],df,'max',100000,1)

    #fill the first instances of agg with 0s
    df.fillna(0, inplace = True)

    #last year drivers and constructors champ stats
    df['last_year_constructors_champ'] = np.where(((df['last_year_and_TeamName_Points'] == df['max_last_year_and_TeamName_Points']) & (df['max_last_year_and_TeamName_Points'] != 0)), True, False)
    df['last_year_drivers_champ'] = np.where(((df['last_year_and_FullName_Points'] == df['max_last_year_and_FullName_Points'])& (df['max_last_year_and_FullName_Points'] != 0)), True, False)

    df['last_year_const_diff'] = df['max_last_year_and_TeamName_Points'] - df['last_year_and_TeamName_Points']
    df['last_year_dive_diff'] = df['max_last_year_and_FullName_Points'] - df['last_year_and_FullName_Points']

    #Drop non-relevent columns
    df.drop(['mean_team_GridPosition','mean_team_Position','GridPosition','positions_gained','Position','TeamName','Points','Status','EventDate','Status_Driver_Error','Status_Car_Failure','Status_Finished','Status_Other'],axis=1,inplace=True)


    return df

race_df = race_agg(race_df)

race_df.to_csv('csv/race_feature_agg.csv')
