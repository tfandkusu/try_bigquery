from google.cloud import bigquery

# シカゴ市の人口2000, 2010, 2020
POPULATIONS = [2896016, 2695598, 2746388]

client = bigquery.Client()

query = """
SELECT
FORMAT('%04d/%02d', EXTRACT(year FROM date),EXTRACT(month FROM date)) as month,
COUNT(unique_key) AS count
FROM `bigquery-public-data.chicago_crime.crime`
GROUP BY month
ORDER BY month ASC
"""
table = client.get_table("tfandkusu.try_bigquery.crime_month")
query_job = client.query(query)

for row in query_job:
    month = row["month"]
    count = row["count"]
    population = 0
    if month < "2010":
        population = POPULATIONS[0]
    elif month < "2020":
        population = POPULATIONS[1]
    else:
        population = POPULATIONS[2]
    if month < "2022/08":
        value = 100000.0 * count / population
        json_rows = [{"month": month, "value": value}]
        errors = client.insert_rows_json(table, json_rows)
        if errors != []:
            print(errors)
            exit(1)
