import pika
import os
import sys
import sqlite3
import json
import csv
import time

host = 'localhost'
queue = 'data_queue'
consumed_queue = 'consumed_queue'
routing_key = 'consumed'
table_name = ""

# this module is updating the sqlite local db
# with the data contained in the selected csv/json file
# then updates the graph consumer to update
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))

    channel = connection.channel()
    channel.queue_declare(queue=queue)

    # getting a callback from the publisher
    # whenever a message is sent
    def callback(ch, method, properties, body):
        file_data = json.loads(body)
        localtime = time.asctime(time.localtime(time.time()))
        print("%s\nData received:\n%s\n" % (localtime, file_data['file_path']))
        prepare(file_data, connection)

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

    print('[-] ACTIVE and waiting for new data [-] \n')
    channel.start_consuming()


# prepare a connection object to local db file
# creates a db file if its not already exist
def prepare(file_data, connection):
    global table_name
    conn = sqlite3.connect('invoices.db')
    cursor = conn.cursor()
    table_name = file_data['table_name']

    validate_table(cursor)
    localtime = time.asctime(time.localtime(time.time()))
    print("%s\nInserting data into the db" % localtime)
    insert_data(conn, cursor, file_data, connection)


# validate that the specific table name
# exists in the local db file
def validate_table(cursor):
    exists = cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    if exists.fetchall()[0][0] == 0:
        # db table created with the specific columns needed for the assignment
        cursor.execute("CREATE TABLE "+table_name+" (customer, date, amount)")


# insert the data values from the file into the db
def insert_data(conn, cursor, file_data, connection):
    file_path = file_data['file_path']
    file_type = file_data['file_type']
    values = []

    if file_type == 'json':
        with open(file_path, mode='r', encoding='utf8') as file:
            data = json.load(file)

            for i in data:
                customer_id = str(i['CustomerId'])
                invoice_date = str(i['InvoiceDate'])
                amount = str(i['Total'])
                value = [customer_id, invoice_date, amount]
                values.append(value)

    else:
        with open(file_path, mode='r', encoding='utf8') as file:
            data = csv.reader(file)
            next(data, None)
            for line in data:
                customer_id = str(line[1])
                invoice_date = str(line[2])
                amount = str(line[8])
                value = [customer_id, invoice_date, amount]
                values.append(value)

    insert_query = "INSERT INTO " + table_name + " (customer, date, amount) VALUES (?, ?, ?)"
    cursor.executemany(insert_query, values)
    conn.commit()
    publish_consumed(connection)


# publish notification on ending the insertion of data
# to the graph consumer
def publish_consumed(connection):
    localtime = time.asctime(time.localtime(time.time()))
    print("%s\nFinished inserting data into db\n" % localtime)
    channel = connection.channel()
    channel.queue_declare(queue=consumed_queue)
    channel.basic_publish(exchange='', routing_key=consumed_queue, body=table_name)
    print('Graph consumer notified\n')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
