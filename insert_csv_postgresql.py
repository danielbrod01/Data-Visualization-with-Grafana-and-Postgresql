import psycopg2
import csv


# Connect to the db
conn = psycopg2.connect(host = "localhost", dbname = "danielrodriguez", user = "danielrodriguez", port = 5432)
# Creates cursor
cur = conn.cursor()

# Create a database table for cars
# cur.execute("""
# CREATE TABLE cars(
# id BIGSERIAL NOT NULL PRIMARY KEY,
# car_make VARCHAR(100) NOT NULL,
# car_model VARCHAR(100) NOT NULL,
# car_year VARCHAR(50) NOT NULL,
# car_color VARCHAR(50) NOT NULL,
# price NUMERIC(19, 2) NOT NULL,
# date DATE NOT NULL,
# sales INT NOT NULL)
# """)

# Create a database table for car_sales
cur.execute("""
CREATE TABLE car_sales(
id BIGSERIAL NOT NULL PRIMARY KEY,
date DATE NOT NULL,
sales INT NOT NULL)
""")
conn.commit() # Commit transaction
# Create a file object named 'file' to read from
csv_file_name = 'car_sales.csv'
with open(csv_file_name, 'r') as file:
    reader = csv.reader(file)
    next(reader) # Skips the header row that contains the column names
    for row in reader:
        cur.execute(
        "INSERT INTO car_sales (date, sales) VALUES (%s, %s)",
        row
        ) # Inserts each row data into database table
    conn.commit() # Commit transaction

# Select all columns from table and print out on console to verify to user that it was successful
cur.execute("SELECT * FROM cars LIMIT 5")

rows = cur.fetchall()

for row in rows:
    print(row)

# Close the cursor
cur.close()
# Close the connection
conn.close()
