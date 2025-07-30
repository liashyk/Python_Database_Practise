import pandas as pd
import psycopg2
import yaml
from pathlib import Path  # Add this import

# Use Path to handle paths in a cross-platform way
config = yaml.safe_load(open(Path("config.yml")))

# Convert paths in config to proper Path objects
students_xlsx = Path(config["students_xlsx"])
students_csv = Path(config["students_csv"])

df = pd.read_excel(students_xlsx)

with psycopg2.connect(host = config["host"],
                        database = config["database"],
                        user = config["user"],
                        password = config["password"]) as con:




    #create table in SQL database

    with con.cursor() as cur:

        cur.execute(f'DROP TABLE IF EXISTS {config["table_name"]}')

        cur.execute(f"""CREATE TABLE {config["table_name"]}
                        (id serial PRIMARY KEY,
                        "first name" varchar(40),
                        "second name" varchar(40),
                        "age" int,
                        "average mark" real,
                        "gender" varchar(40),
                        "phone_number" varchar(25))""")



        # Remove rows where 'average mark' is missing.
        new_df = df.dropna(subset=['average mark']).copy()


        # Split 'student name' into two new columns
        new_df[['first name', 'second name']] = new_df['student name'].str.split(n=1, expand=True)

        # Drop the original 'student name' column
        new_df.drop(columns='student name', inplace=True)

        #write students in csv format
        new_df.to_csv(students_csv, index=False)

        #Insert data into the 'students' table in your DB.
        with open(students_csv, 'r') as f:
            next(f)
            cur.copy_from(f, config["table_name"], sep=',', columns=(
                "age", "average mark", "gender", "phone_number", "first name", "second name"
            ))

        con.commit()


        #Count the number of students
        cur.execute(f"""
                        SELECT "gender", COUNT(*) 
                        FROM "students" 
                        WHERE "average mark" > 5
                        GROUP BY "gender"
                    """)

        sql_result = cur.fetchall()

        gender_count = pd.DataFrame(sql_result, columns=["gender", "count"])

        print(gender_count)