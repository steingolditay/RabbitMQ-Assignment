#!/usr/bin/env python
import pika
import json
import sys
import os
import time

host = 'localhost'
queue = 'data_queue'
table_name = 'invoices'

# this module expects exactly one argument
# representing the path to the [csv or json only] file
# from which the data should be loaded
def main():
    number_of_args = len(sys.argv)
    if number_of_args > 2:
        print("Too many arguments. only one argument expected [json/csv file path]")

    elif number_of_args < 2:
        print("Missing argument [json/csv file path]")

    # this module expects exactly one argument
    # representing the path to csv or json file
    # from which the data should be loaded
    else:
        file = sys.argv[1]
        if not (file.endswith('.json') or file.endswith('.csv')):
            print("File not supported. Use only json or csv files.")

            # initialize the process of getting the file's data
            # and handing it to the message_consumer module
        else:
            data = {'file_path': os.path.abspath(file),
                    'table_name': table_name}
            if file.endswith('.json'):
                data['file_type'] = 'json'

            else:
                data['file_type'] = 'csv'
            # parsing the message's body data as json
            message = json.dumps(data)
            send_message(message, data['file_path'])


# create a connection channel with pika
# publish the file's data
# then close the connection
def send_message(message, file_path):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()

    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=queue, body=message)

    localtime = time.asctime(time.localtime(time.time()))
    print("%s\nSent file data to message consumer: \n%s" % (localtime, file_path))
    connection.close()


if __name__ == '__main__':
    main()
