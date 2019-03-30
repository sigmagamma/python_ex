# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 22:09:30 2019

@author: shaig
This module sends a message to a queue. See readme for further documentation
"""

import pika
import sys

QUEUE_NAME = 'chinook'

if __name__ == "__main__":
    if len(sys.argv) < 5:
        raise ValueError("Missing Arguments")
    server = sys.argv[1]
    path = sys.argv[2]
    country = sys.argv[3]
    year = sys.argv[4]
    if not year.isnumeric():
        raise ValueError("Incorrect year argument")
    message = path +" "+ country + " " + year
    connection = pika.BlockingConnection(pika.ConnectionParameters(server))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_publish(exchange='',
                          routing_key=QUEUE_NAME,
                          body=message)
    connection.close()