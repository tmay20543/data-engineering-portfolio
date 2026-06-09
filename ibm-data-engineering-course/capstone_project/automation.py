# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to PostgreSQL
import psycopg2

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    user='root',
    password='bMBqwGsKf6rvGFvLUEgeh9Rc',
    host='172.21.156.122',
    database='sales'
)
mysql_cursor = mysql_connection.cursor()

# Connect to PostgreSQL
dsn_hostname = '172.21.98.64'
dsn_user = 'postgres'
dsn_pwd = '2NgHbdnAgtYmB6AvyTPu0ag5'
dsn_port = "5432"
dsn_database = "sales"

pg_connection = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)
pg_cursor = pg_connection.cursor()


def get_last_rowid():
    query = "SELECT MAX(rowid) FROM sales_data"
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()

    if result[0] is None:
        return 0

    return result[0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)


def get_latest_records(rowid):
    query = """
        SELECT rowid, product_id, customer_id, quantity
        FROM sales_data
        WHERE rowid > %s
    """
    mysql_cursor.execute(query, (rowid,))
    records = mysql_cursor.fetchall()
    return records


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))


def insert_records(records):
    if len(records) == 0:
        return

    query = """
        INSERT INTO sales_data
        (rowid, product_id, customer_id, quantity)
        VALUES (%s, %s, %s, %s)
    """

    for row in records:
        pg_cursor.execute(query, row)

    pg_connection.commit()


insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))


# disconnect from mysql warehouse
mysql_cursor.close()
mysql_connection.close()

# disconnect from PostgreSQL data warehouse
pg_cursor.close()
pg_connection.close()

# End of program