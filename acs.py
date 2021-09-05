import requests
from Config import config
import pandas as pd
import psycopg2


def getData(state, variables):
    # https://api.census.gov/data/2019/acs/acs5/groups.html -Data Key
    key = "38f005de4366d6c599049a090361753b4a05dad1"
    print(f"Getting {variables} for {state}")
    # https://api.census.gov/data/2019/acs/acs5/examples.html -API examples with varying levels of Geography
    url = f"https://api.census.gov/data/2019/acs/acs5?get={variables}&for=block%20group:*&in=state:{state}%20county:*&key={key}"

    return requests.get(url).json()


# B01001_002E	Estimate!!Total:!!Male:
# B01001_026E	Estimate!!Total:!!Female:
# B19083_001E	Estimate!!Gini Index
# B01002_001E	Estimate!!Median age --!!Total:

# B01003_001E	Estimate!!Total

# B02001_001E	Estimate!!Total:
# B02001_002E	Estimate!!Total:!!White alone
# B02001_003E	Estimate!!Total:!!Black or African American alone
# B02001_004E	Estimate!!Total:!!American Indian and Alaska Native alone
# B02001_005E	Estimate!!Total:!!Asian alone
# B02001_006E	Estimate!!Total:!!Native Hawaiian and Other Pacific Islander alone
# B02001_007E	Estimate!!Total:!!Some other race alone

# B19013_001E	Estimate!!Median household income in the past 12 months (in 2019 inflation-adjusted dollars)

def buildDF(st, listofVars, listofVarNames):
    var_dict = dict(zip(listofVars, listofVarNames))
    str_vars = ",".join(listofVars)

    data = getData(st, str_vars)
    insert_df = pd.DataFrame(data, columns=data[0])
    insert_df = insert_df.convert_dtypes()
    insert_df = insert_df.rename(columns=var_dict)
    print(insert_df.dtypes)
    insert_df.drop(index=insert_df.index[0], axis=0, inplace=True)
    print(insert_df.shape)
    print("Writing DF to CSV...")
    insert_df.to_csv('upload_data_from_copy.csv', index=False, header=False)


def connectToDB():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def createTable(conn):
    print("Creating Table...")
    try:
        cur = conn.cursor()
        create_sql = """CREATE TABLE acs.kjharris_acs_data (
                Male INTEGER,
                Female INTEGER,
                Median_Age REAL,
                Total_Pop INTEGER,
                RACE_White INTEGER,
                RACE_Black INTEGER,
                RACE_Asian INTEGER,
                HH_Med_Income INTEGER,
                state INTEGER,
                county INTEGER,
                tract INTEGER,
                block_grp INTEGER,
                PRIMARY KEY (state,county,tract,block_grp)
                );"""

        cur.execute("DROP TABLE IF EXISTS acs.kjharris_acs_data")
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insertIntoTable(conn):
    try:
        cur = conn.cursor()
        cur.execute("SET search_path TO acs,public;")
        f = open('upload_data_from_copy.csv', 'r')
        print("Copying data from CSV to Table... ")
        cur.copy_from(file=f, table="kjharris_acs_data", sep=",")
        cur.execute("Select COUNT(*) FROM acs.kjharris_acs_data;")
        print(f"Number of Rows inserted = {cur.fetchone()[0]}")
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def main():
    varCodes = ["B01001_002E", "B01001_026E", "B01002_001E", "B01003_001E", "B02001_002E", "B02001_003E", "B02001_005E",
                "B19013_001E"]
    varNames = ["Male", "Female", "Median_Age", "Total_Pop", "RACE_White", "RACE_Black", "RACE_Asian",
                "HH_Median_Income"]
    state = input("What State do you want to gather data for? Input state FIPS code. Example AL = 01. ")
    buildDF(state, varCodes, varNames)
    conn = connectToDB()
    createTable(conn)
    insertIntoTable(conn)
    print("Finished. Closing connection to DB.. ")
    conn.close()


if __name__ == '__main__':
    main()
