import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
race_df = pd.read_csv('csv/race_bronze_df.csv')
quali_df = pd.read_csv('csv/qualifying_bronze_df.csv')
### Preprocessing

def race_status_cat(df: pd.DataFrame):

    df["Status"] = ["Finished" if "Lap" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Driver_Error" if "Accident" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Driver_Error" if "Collision" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Driver_Error" if "Spun off" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Driver_Error" if "Collision damage" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Other" if "Withdrew" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Other" if "Did not qualify" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Other" if "Not classified" in ele else ele for ele in df["Status"]]
    df["Status"] = ["Other" if "Disqualified" in ele else ele for ele in df["Status"]]
    df["Status"] = [ele if (("Finished" in ele) or ("Driver_Error" in ele) or ("Other" in ele)) else "Car_Failure" for ele in df["Status"]]

    return df

def remove_dup(df: pd.DataFrame):

    norig = df.shape[0]
    df_uniqueid = pd.DataFrame({'count' : df.groupby(['FullName','event','year']).size()}).reset_index()
    df = df.merge(df_uniqueid, how='left', on =['FullName','event','year'])
    df = df[(df['count'] == 1)]
    df.drop('count',axis=1,inplace=True)
    nfin = df.shape[0]

    print(f'Number of duplicates removed: {norig - nfin}')

    return df

def drop_col_and_na(drop_list:list, df: pd.DataFrame):

    norig =  df.shape[0]
    corig = df.shape[1]

    df = df.drop(drop_list,axis=1)
    df.dropna(inplace = True)

    nfin = df.shape[0]
    cfin = df.shape[1]

    print(f'Number of columns removed: {corig - cfin}')
    print(f'Number of na removed: {norig - nfin}')

    return df

def timedelt_conv(df:pd.DataFrame,convert_list):

    for col in convert_list:
        df[col] = pd.to_timedelta(df[col])

    return df

def final_quali_time(df):

    df['final_quali_time'] = np.nan
    df['final_quali_time'] = df['final_quali_time'].fillna(df.pop('Q3'))
    df['final_quali_time'] = df['final_quali_time'].fillna(df.pop('Q2'))
    df['final_quali_time'] = df['final_quali_time'].fillna(df.pop('Q1'))
    df['final_quali_time'] = [t.total_seconds() for t in df['final_quali_time']]

    df['best_quali_time'] = df.groupby(['event','year'])['final_quali_time'].transform(min)

    df['rel_time_delta'] = (df['final_quali_time'] - df['best_quali_time']) / df['best_quali_time']

    return df

race_df = remove_dup(race_df)
race_df = race_status_cat(race_df)

print(race_df.groupby(['Status'])["FullName"].count())

viz_race_df = drop_col_and_na(['DriverNumber','BroadcastName','FirstName','LastName','Time','Q1','Q2','Q3'],race_df)
viz_race_df.to_csv('csv/race_viz_silver_df.csv')

race_df = drop_col_and_na(['DriverNumber','BroadcastName','Abbreviation','TeamColor','FirstName','LastName','Time','Q1','Q2','Q3'],race_df)
race_df.to_csv('csv/race_silver_df.csv')


quali_df = timedelt_conv(quali_df,["Q1","Q2","Q3"])

quali_df = final_quali_time(quali_df)

viz_quali_df = drop_col_and_na(['Unnamed: 0','TeamName','DriverNumber','BroadcastName','FirstName','LastName','Time','Position','GridPosition','Status','Points','session'], quali_df)
viz_quali_df.to_csv('csv/quali_viz_silver_df.csv')

quali_df = drop_col_and_na(['Unnamed: 0','TeamName','DriverNumber','BroadcastName','Abbreviation','TeamColor','FirstName','LastName','Time','Position','GridPosition','Status','Points','session'], quali_df)
quali_df.to_csv('csv/quali_silver_df.csv')



