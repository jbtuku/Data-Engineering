import psycopg2
import csv
import argparse
import time


DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName = "census"

def connect():
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

def convert_type(value, target_type):
    try:
        if value == "" or value is None:
            return None
        if target_type == int:
            return int(float(value))
        return target_type(value)
    except:
        return None

def load_data(conn, filename):
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rowcount = 0
        start = time.perf_counter()

        with conn.cursor() as cur:
            for row in reader:
                try:
                    values = [
                        convert_type(row['TractId'], int),
                        row['State'],
                        row['County'],
                        convert_type(row['TotalPop'], int),
                        convert_type(row['Men'], int),
                        convert_type(row['Women'], int),
                        convert_type(row['Hispanic'], float),
                        convert_type(row['White'], float),
                        convert_type(row['Black'], float),
                        convert_type(row['Native'], float),
                        convert_type(row['Asian'], float),
                        convert_type(row['Pacific'], float),
                        convert_type(row['VotingAgeCitizen'], int),
                        convert_type(row['Income'], int),
                        convert_type(row['IncomeErr'], int),
                        convert_type(row['IncomePerCap'], int),
                        convert_type(row['IncomePerCapErr'], int),
                        convert_type(row['Poverty'], float),
                        convert_type(row['ChildPoverty'], float),
                        convert_type(row['Professional'], float),
                        convert_type(row['Service'], float),
                        convert_type(row['Office'], float),
                        convert_type(row['Construction'], float),
                        convert_type(row['Production'], float),
                        convert_type(row['Drive'], float),
                        convert_type(row['Carpool'], float),
                        convert_type(row['Transit'], float),
                        convert_type(row['Walk'], float),
                        convert_type(row['OtherTransp'], float),
                        convert_type(row['WorkAtHome'], float),
                        convert_type(row['MeanCommute'], float),
                        convert_type(row['Employed'], int),
                        convert_type(row['PrivateWork'], float),
                        convert_type(row['PublicWork'], float),
                        convert_type(row['SelfEmployed'], float),
                        convert_type(row['FamilyWork'], float),
                        convert_type(row['Unemployment'], float),
                    ]

                    cur.execute(
                        f"INSERT INTO {TableName} VALUES ({','.join(['%s'] * len(values))})", values
                    )
                    rowcount += 1

                except Exception as e:
                    print(f"Skipping bad row: {row}")
                    print(f"Error: {e}\n")

            
            conn.commit()

        elapsed = time.perf_counter() - start
        print(f"Inserted {rowcount} rows in {elapsed:.2f} seconds")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datafile', required=True)
    parser.add_argument('-c', '--create', action='store_true')
    args = parser.parse_args()

    conn = connect()
    if args.create:
        create_table(conn)
    load_data(conn, args.datafile)
    conn.close()

if __name__ == "__main__":
    main()