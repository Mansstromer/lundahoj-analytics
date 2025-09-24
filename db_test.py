import psycopg2, pandas as pd

conn = psycopg2.connect(
    dbname="lundahoj",
    user="postgres",
    password="4268",
    host="localhost",
    port=5432
)

df = pd.read_sql("SELECT * FROM public.weather LIMIT 10;", conn)
print(df.head())