# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 22:13:52 2019

@author: shaig
This module parses a message from a queue. See readme for further documentation
"""

import pika
import sys
import csv
import json
import sqlite3
import xml.etree.ElementTree as ET

#constants
QUEUE_NAME = 'chinook'
CSV_DELIMITER = ','

##### WRITER FUNCTIONS ######

def write_csv(target_file, source_iterator):
    # different syntax for python 3, use wb and no newline for python 2
    with open(target_file,mode='w',newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=CSV_DELIMITER, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in source_iterator:
            csv_writer.writerow(list(row))
            
def write_json(target_file, source_dict):
    # different syntax for python 3, use wb and no newline for python 2
    with open(target_file,mode='w',newline='') as json_file:
        json_file.write(json.dumps(source_dict, indent=4,ensure_ascii=False))
def write_xml(target_file, source_element_tree):
    data = ET.tostring(source_element_tree,encoding = 'unicode')
    with open(target_file,mode='w',newline='') as xml_file:
        xml_file.write(data)   

##### DATA STRUCTURES FOR OUTPUT #####     
# allows receiving output from cursor as dictionary
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def create_country_albums_dict(source_iterator):
    previous_country = ""
    albums = []
    countries = []
    for row_dict in source_iterator:
        current_country = row_dict.pop("BillingCountry")
        if current_country == previous_country:
            albums.append(row_dict)
        else:
            country = {"Name":previous_country,"Album":albums}
            countries.append(country)
            previous_country = current_country
            albums = [row_dict]
    country_dict = {"Country":countries}
    return country_dict
def create_track_element_tree(cursor,input_year):
    track = ET.Element('Track')
    name = ET.SubElement(track,'Name')
    country = ET.SubElement(track,'Country')
    sales = ET.SubElement(track,'Sales')
    year = ET.SubElement(track,'Year')
    year.text = str(input_year)
    row = cursor.fetchone()
    if row is None:
        return None
    else:
        country.text = row[0]
        name.text = row[2]
        sales.text = str(row[3])
        return track     
##### QUERIES and CRUD
def find_purchases_by_country(cursor,write_to_db):
    query = """SELECT BillingCountry,count(InvoiceId)
                  FROM invoices GROUP BY BillingCountry"""
    if write_to_db == True:
        cursor.execute("""DROP TABLE IF EXISTS purchases_ex""")
        query = (""" CREATE TABLE purchases_ex AS """ + query)
    return cursor.execute(query)
def find_items_by_country(cursor,write_to_db):
    query = """SELECT BillingCountry,count(InvoiceLineId) FROM invoices 
                   JOIN invoice_items ON invoice_items.InvoiceId = invoices.InvoiceId
                   GROUP BY BillingCountry"""
    if write_to_db == True:
        cursor.execute("""DROP TABLE IF EXISTS items_ex""")
        query = (""" CREATE TABLE items_ex AS """ + query)
    return cursor.execute(query)
def find_albums_by_country(cursor):
    return cursor.execute("""
                    SELECT DISTINCT BillingCountry, albums.AlbumId, albums.Title FROM invoices 
                    JOIN invoice_items ON invoice_items.InvoiceId = invoices.InvoiceId,
                    tracks ON invoice_items.TrackId = tracks.TrackId,
                    albums ON tracks.AlbumId = albums.AlbumId
                    ORDER BY BillingCountry, albums.AlbumId """)
def find_track_by_parameters(cursor, country, year,write_to_db):
# fetches the track with the most sales for country in given parameters
    query = """SELECT invoices.BillingCountry, tracks.TrackId, tracks.Name, 
                          sum(quantity) as Sales 
                          FROM invoices 
                          JOIN invoice_items ON invoice_items.InvoiceId = invoices.InvoiceId, 
                          tracks ON invoice_items.TrackId = tracks.TrackId
                          WHERE BillingCountry  = ? AND 
                          cast(strftime('%Y',invoices.InvoiceDate) AS INTEGER) > ?
                          GROUP BY tracks.TrackId
                          ORDER BY Sales DESC
                          LIMIT 1 """
    if write_to_db == True:
        cursor.execute("""DROP TABLE IF EXISTS track_ex""")
        query = (""" CREATE TABLE track_ex AS """ + query)
    cursor.execute(query,(country,year))
                    
#callback when receiving message
def callback(ch, method, properties, body):
    # parse input
    bodylist = body.decode("utf-8").split(" ")
    path = bodylist[0]
    country = bodylist[1]
    year = bodylist[2]
    
    #connect to db
    conn = sqlite3.connect(path)
    c = conn.cursor()
    
    #1. create csv file with purchases by country
    write_csv('purchasesbycountry.csv', find_purchases_by_country(c,False))
    
    #2. create csv file with items by country
    write_csv('itemsbycountry.csv', find_items_by_country(c,False))
    
    #3. create json file with albums by country
    old_factory = conn.row_factory
    conn.row_factory = dict_factory
    c = conn.cursor()
    albums_dict = create_country_albums_dict(find_albums_by_country(c))
    write_json('albumsbycountry.json',albums_dict)
    
    #4. create xml file with top track for given country and sales by year
    conn.row_factory = old_factory
    c = conn.cursor()
    find_track_by_parameters(c,country,year,False)
    track_element_tree = create_track_element_tree(c,year)
    if (track_element_tree is not None):
        write_xml('tracksales.xml',track_element_tree) 
    
    #1. write to DB
    find_purchases_by_country(c,True)
    find_items_by_country(c,True)
    find_track_by_parameters(c,country,year,True)
#main method
if __name__ == "__main__":
    if len(sys.argv) < 1:
        raise ValueError("Missing Arguments")
    server = sys.argv[1]
    connection = pika.BlockingConnection(pika.ConnectionParameters(server))
    channel = connection.channel()
    channel.queue_delete(queue=QUEUE_NAME)
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_consume(on_message_callback=callback,
                      queue=QUEUE_NAME)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()    

