import pandas as pd
import time
from config import bdd_connection, engine_conn


def create_table_psycopg2(name_table, columns):
    with bdd_connection() as conn:
        cur = conn.cursor()

        create_table = f"""CREATE TABLE IF NOT EXISTS public.{name_table} ()"""
        cur.execute(create_table)
        conn.commit()
        # alter table
        alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY"""
        cur.execute(alter_table)
        conn.commit()
        for column in columns:
            alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS {column} varchar(255)"""
            cur.execute(alter_table)
            conn.commit()



def add_row_csv(name, data):
    df = pd.read_csv("../File csv eurostats/csv/search_for_insert/" + name + ".csv", sep=",")
    last_id = df['id'].iloc[-1]
    data.insert(0, last_id + 1)
    df.loc[len(df.index)] = data

    df.to_csv("../File csv eurostats/csv/search_for_insert/" + name + ".csv", index=False)


def add_row_sql(name, data):
    with bdd_connection() as conn:
        cur = conn.cursor()

        setences = "( "
        for value in data:
            setences += f"{value}, "
        setences = setences[:-2]
        setences += " )"

        insert = f"""INSERT INTO public.{name} VALUES {setences}"""
        cur.execute(insert)
        conn.commit()

def make_row_to_insert(row):
    # search id

    # test value is not nan
    # also add new row

    # return (id...., id...., row[""], .....)
    return (row)

def insert_engine():
    path = ""
    df = pd.read_csv(path + 'XXX.csv')

    print(len(df.index))

    time_start = time.time()

    # create table and columns
    name_table = ""
    columns = [""]
    create_table_psycopg2(name_table, columns)

    with engine_conn() as conn:

        for i in range(0, len(df.index), 50000):
            time_start_loop = time.time()

            df_insert = df.iloc[i:i + 50000].apply(lambda row: pd.Series(make_row_to_insert(row)), axis=1)

            df_insert.columns = columns

            df_insert.to_sql(name_table, conn, if_exists='append', index=False)
            conn.commit()
            print("time : ", time.time() - time_start_loop)

        print("time end : ", time.time() - time_start)



def insert_psycopg2():
    path = ""
    df = pd.read_csv(path + 'XXX.csv')

    print(len(df.index))

    time_start = time.time()

    # create table and columns
    name_table = ""
    columns = [""]
    create_table_psycopg2(name_table, columns)

    [add_row_sql(name_table, row) for index, row in df.iterrows()]

    print("time end : ", time.time() - time_start)