import requests
from Config import config
import pandas as pd
import psycopg2


def getData(state, variable):
    #https://api.census.gov/data/2019/acs/acs5/groups.html -Data Key
    key="38f005de4366d6c599049a090361753b4a05dad1"
    print(f"Getting {variable} for {state}")
    url = f"https://api.census.gov/data/2019/acs/acs5?get=NAME,{variable}&for=block%20group:*&in=state:{state}%20county:*"
    return requests.get(url).json()

#B01001_002E	Estimate!!Total:!!Male:
#B01001_026E	Estimate!!Total:!!Female:

#B01002_001E	Estimate!!Median age --!!Total:

#B01003_001E	Estimate!!Total

#B02001_001E	Estimate!!Total:
#B02001_002E	Estimate!!Total:!!White alone
#B02001_003E	Estimate!!Total:!!Black or African American alone
#B02001_004E	Estimate!!Total:!!American Indian and Alaska Native alone
#B02001_005E	Estimate!!Total:!!Asian alone
#B02001_006E	Estimate!!Total:!!Native Hawaiian and Other Pacific Islander alone
#B02001_007E	Estimate!!Total:!!Some other race alone

#B19013_001E	Estimate!!Median household income in the past 12 months (in 2019 inflation-adjusted dollars)

#hh_inc = getData('42','B19013_001E')

def buildDF(st,listofVars,listofVarNames):
    var_dict = dict(zip(listofVars, listofVarNames))
    InsertDF = pd.DataFrame()
    for var in listofVars:
        data = getData(st,var)
        df = pd.DataFrame(data, columns=data[0])
        df = df.drop(columns="NAME",inplace=False)
        df = df.convert_dtypes()
        df = df.set_index(['state', 'county', 'tract', 'block group'])

        if (InsertDF.shape == (0,0)):
            InsertDF = df
        else:
            InsertDF = InsertDF.join(df, how='inner')
    InsertDF = InsertDF.rename(columns=var_dict)
    InsertDF = InsertDF.reset_index()
    InsertDF = InsertDF.dropna()
    print(InsertDF.dtypes)
    InsertDF.to_csv('upload_test_data_from_copy.csv', index=False, header=False)
    return InsertDF

def connectToDB():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn =  psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def createTable(conn):
    try:
        cur = conn.cursor()
        create_sql = """CREATE TABLE IF NOT EXISTS acs.kjharris_acs_data (
                state INTEGER,
                county INTEGER,
                tract INTEGER,
                block_grp INTEGER,
                Male INTEGER,
                Female INTEGER,
                Median_Age REAL,
                Total_Pop INTEGER,
                RACE_WHITE INTEGER,
                RACE_Tot INTEGER,
                HH_Med_Income INTEGER,
                PRIMARY KEY (county,tract,block_grp)
                );"""
        cur.execute(create_sql)
        cur.execute("TRUNCATE TABLE acs.kjharris_acs_data")

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def InsertIntoDB(conn):
        # create a cursor
        cur = conn.cursor()
        cur.execute("SET search_path TO acs,public;")
        f = open('upload_test_data_from_copy.csv', 'r')
        cur.copy_from(file=f, table="kjharris_acs_data", sep=",")

        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()


if __name__ == '__main__':
    varCodes = ["B01001_002E", "B01001_026E", "B01002_001E", "B01003_001E", "B02001_002E", "B02001_001E", "B19013_001E"]
    varNames = ["Male", "Female", "Median_Age", "Total_Pop", "RACE_White", "RACE_Tot", "HH_Median_Income"]

    #df = buildDF('42', varCodes, varNames)
    conn = connectToDB()
    createTable(conn)
    InsertIntoDB(conn)
