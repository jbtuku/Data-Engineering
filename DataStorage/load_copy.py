import time
import psycopg2
import argparse
import csv
import io

# Database configuration
DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName = "census"
Datafile = "filedoesnotexist"  # to be replaced via CLI
CreateDB = False  # will be set via CLI


def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile, CreateDB
    Datafile = args.datafile
    CreateDB = args.createtable


def dbconnect():
    conn = psycopg2.connect(
        dbname=DBname,
        user=DBuser,
        password=DBpwd,
        host="localhost"
    )
    return conn


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TableName};")
        cur.execute(f"""
            CREATE TABLE {TableName} (
                TractId BIGINT,
                State TEXT,
                County TEXT,
                TotalPop INTEGER,
                Men INTEGER,
                Women INTEGER,
                Hispanic FLOAT,
                White FLOAT,
                Black FLOAT,
                Native FLOAT,
                Asian FLOAT,
                Pacific FLOAT,
                VotingAgeCitizen INTEGER,
                Income INTEGER,
                IncomeErr INTEGER,
                IncomePerCap INTEGER,
                IncomePerCapErr INTEGER,
                Poverty FLOAT,
                ChildPoverty FLOAT,
                Professional FLOAT,
                Service FLOAT,
                Office FLOAT,
                Construction FLOAT,
                Production FLOAT,
                Drive FLOAT,
                Carpool FLOAT,
                Transit FLOAT,
                Walk FLOAT,
                OtherTransp FLOAT,
                WorkAtHome FLOAT,
                MeanCommute FLOAT,
                Employed INTEGER,
                PrivateWork FLOAT,
                PublicWork FLOAT,
                SelfEmployed FLOAT,
                FamilyWork FLOAT,
                Unemployment FLOAT
            );
        """)
        print(f"Created table: {TableName}")


def read_data(filename):
    print(f"Reading from: {filename}")
    with open(filename, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_data_with_copy_from(conn, rows):
    with conn.cursor() as cur:
        print(f"Loading {len(rows)} rows using copy_from()...")
        start = time.perf_counter()

        buffer = io.StringIO()
        for row in rows:
            values = [
                row['TractId'] or '',
                row['State'] or '',
                row['County'].replace("'", "") if row['County'] else '',
                row['TotalPop'] or '',
                row['Men'] or '',
                row['Women'] or '',
                row['Hispanic'] or '',
                row['White'] or '',
                row['Black'] or '',
                row['Native'] or '',
                row['Asian'] or '',
                row['Pacific'] or '',
                row['VotingAgeCitizen'] or '',
                row['Income'] or '',
                row['IncomeErr'] or '',
                row['IncomePerCap'] or '',
                row['IncomePerCapErr'] or '',
                row['Poverty'] or '',
                row['ChildPoverty'] or '',
                row['Professional'] or '',
                row['Service'] or '',
                row['Office'] or '',
                row['Construction'] or '',
                row['Production'] or '',
                row['Drive'] or '',
                row['Carpool'] or '',
                row['Transit'] or '',
                row['Walk'] or '',
                row['OtherTransp'] or '',
                row['WorkAtHome'] or '',
                row['MeanCommute'] or '',
                row['Employed'] or '',
                row['PrivateWork'] or '',
                row['PublicWork'] or '',
                row['SelfEmployed'] or '',
                row['FamilyWork'] or '',
                row['Unemployment'] or ''
            ]
            buffer.write('|'.join(values) + '\n')

        buffer.seek(0)
        cur.copy_from(buffer, TableName, sep='|', null='')

        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"Finished loading. Elapsed time: {elapsed:.2f} seconds")


def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
        create_table(conn)

    rows = read_data(Datafile)
    load_data_with_copy_from(conn, rows)
    conn.close()


if __name__ == "__main__":
    main()