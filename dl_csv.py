import pymysql
import csv

# Database connection parameters
db_host = 'localhost'  # Replace with your database host
db_user = 'root'   # Replace with your database username
db_password = '' # Replace with your database password
db_name = ''  # Replace with your database name
table_name = ''  # Replace with the name of the table you want to export
csv_file_path = 'datalogs.csv' # Replace with the desired path to save the CSV file

# Establish a connection
try:
    conn = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
    cur = conn.cursor()

    # Execute the query
    sql = f"SELECT * FROM {table_name}"
    cur.execute(sql)

    # Fetch the results
    results = cur.fetchall()

    # Write to CSV
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Optional: Write the column headers to the CSV file
        headers = [i[0] for i in cur.description]  # Get column names
        csvwriter.writerow(headers)
        # Write the data rows
        csvwriter.writerows(results)

    print(f"Successfully exported data from table '{table_name}' to '{csv_file_path}'")

except pymysql.Error as e:
    print(f"Error: {e}")
finally:
    if conn:
        cur.close()
        conn.close()
