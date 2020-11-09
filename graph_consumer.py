import pika
import os
import sys
import sqlite3
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots as subplots
import plotly.offline
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
import time
import threading

host = 'localhost'
consumed_queue = 'consumed_queue'

options = webdriver.ChromeOptions()
driver = ''
is_showing = False


# this module gather the data from the updated table
# and uses Selenium to show a Plotly graph in Chrome browser
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=consumed_queue)

    # getting a callback from the message_consumer
    # whenever new data is inserted into the local db
    def callback(ch, method, properties, body):
        localtime = time.asctime(time.localtime(time.time()))
        table_name = body.decode('utf-8')
        print("%s\nUpdating graph for table: '%s'\n" % (localtime, table_name))
        get_data(table_name)

    channel.basic_consume(queue=consumed_queue, on_message_callback=callback, auto_ack=True)

    print('[-] ACTIVE and waiting for new consumed data [-] \n')
    channel.start_consuming()


def get_data(table_name):
    conn = sqlite3.connect('invoices.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM "+table_name+" ")
    table_data = cursor.fetchall()

    build_graph(table_data)


# build an updated graph summing up the data
# and arranging it from the local db file
def build_graph(table_data):
    data = {}

    # create a dictionary of the summed up data:
    # data = {year: {month: [[customers], total_amount}]}
    for row in table_data:
        customer = row[0]
        date = row[1]
        amount = float(row[2])

        dt = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        year = str(dt.year)
        month = str(dt.month)

        if year not in data:
            data[year] = {}
            data[year][month] = [[customer], amount]
        elif month not in data[year]:
            data[year][month] = [[customer], amount]
        else:
            customers = data[year][month][0]
            current_amount = data[year][month][1]
            if customer not in customers:
                customers.append(customer)
            data[year][month] = customers, amount + current_amount

    # arrange the summed up data in relational lists
    years_list = []
    months_list = []
    active_customers_list = []
    total_amounts_list = []

    for i in range(len(data)):
        position = i
        year = list(data)[position]
        years_list.insert(position, year)
        months_keys = []
        active_keys = []
        amounts_keys = []
        for month in data[year]:
            months_keys.append(month)
            active_keys.append(str(len(data[year].get(month)[0])))
            amounts_keys.append(int(round(data[year].get(month)[1], 2)))

        months_list.insert(position, months_keys)
        active_customers_list.insert(position, active_keys)
        total_amounts_list.insert(position, amounts_keys)

    # create Plotly figures from the lists
    # Plotly exports an html file containing the graph
    # which is then presented with Selenium
    fig = subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)
    for i in range(len(years_list)):
        fig.add_trace(go.Bar(
            x=months_list[i],
            y=total_amounts_list[i],
            name=years_list[i] + " - Total Amount",
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=months_list[i],
            y=active_customers_list[i],
            name=years_list[i] + " - Active Users",
        ), row=2, col=1)
    fig.update_xaxes(title_text="Months", row=2, col=1)

    fig.update_yaxes(title_text="Total Amount per year", row=1, col=1)
    fig.update_yaxes(title_text="Active Users per year", row=2, col=1)

    fig.update_layout(barmode='group', title_text='Invoices Analysis', height=800, width=800)
    plotly.offline.plot(fig, filename='graph.html', auto_open=False)

    # if selenium is already showing a webpage, just refresh it
    # to load the new html file, otherwise, start a new thread
    # raise exception and reload window if closed while running
    if is_showing:
        try:
            driver.refresh()
        except WebDriverException:
            thread = threading.Thread(target=show_graph).start()
    else:
        thread = threading.Thread(target=show_graph).start()


def show_graph():
    global driver
    global is_showing

    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    target_url = os.path.abspath("graph.html")
    driver.get(target_url)
    is_showing = True


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
