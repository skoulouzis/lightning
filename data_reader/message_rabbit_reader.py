#!/usr/bin/env python
import urllib2 
import base64
import json
import time
import requests
import sys
import urllib
import pika
import time
import traceback
import logging


def init_chanel(queue_name,rabbitmq_host):    
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()

        channel.queue_declare(queue=q_name, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=q_name)
        return channel
    except Exception, err:
        # Just print traceback
        print "something went wrong, here is some info:"
        traceback.print_exc()

        # Get traceback as a string and do something with it
        error = traceback.format_exc()
        print error.upper()

        # Log it through logging channel
        logging.error('Ooops', exc_info=True)
        sys.exit(-1)

    
def write_to_influx(observations):
    try:
        influx_base_url = 'http://'+INFLUX_HOST+':'+INFLUX_PORT
        create_db_influx_url = influx_base_url+'/query?q=CREATE DATABASE '+INFLUX_DB
        r = requests.post(create_db_influx_url)
        data_string = ""
        for observation in observations:
            sensor = observation['madeBySensor']    
            unit = observation['resultUnit']
            if 'qualityOfObservation' in observation:
                quality = observation['qualityOfObservation']
            else:
                quality = 1
            value = observation['resultValue']    
            time = observation['resultTime']
            millis = int(round(float(time) * 1000))
            data_string += 'observations,sensor='+sensor+',unit='+unit+',quality='+str(quality)+' value='+str(value) +' '+str(millis)+'\n'
            print data_string

        r = requests.post(influx_base_url+'/write?consistency=one&precision=ms&db='+INFLUX_DB, data=data_string)
        print r
    except Exception, err:
        # Just print traceback
        print "something went wrong, here is some info:"
        traceback.print_exc()

        # Get traceback as a string and do something with it
        error = traceback.format_exc()
        print error.upper()

        # Log it through logging channel
        logging.error('Ooops', exc_info=True)
        sys.exit(-1)    
    
    
def on_request(ch, method, props, body):
    try:
        handle_delivery(body)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)      
    except Exception, err:
        # Just print traceback
        print "something went wrong, here is some info:"
        traceback.print_exc()

        # Get traceback as a string and do something with it
        error = traceback.format_exc()
        print error.upper()

        # Log it through logging channel
        logging.error('Ooops', exc_info=True)
        sys.exit(-1)       
    
    
    
def handle_delivery(message):
    try:
        parsed_json_message = json.loads(message)
        messages = parsed_json_message['messages']
        all_observations = []
        for message in messages:
            observations = json.loads(base64.b64decode(message['data']).decode('utf-8', 'ignore'))
            for observation in observations:
                all_observations.append(observation)
            
        write_to_influx(all_observations)
    except Exception, err:
        # Just print traceback
        print "something went wrong, here is some info:"
        traceback.print_exc()

        # Get traceback as a string and do something with it
        error = traceback.format_exc()
        print error.upper()

        # Log it through logging channel
        logging.error('Ooops', exc_info=True)
        sys.exit(-1)       
    
        

       
        
    
def callback(ch, method, properties, body):
    try:
        handle_delivery(body)
        ch.basic_ack(delivery_tag = method.delivery_tag)
    except Exception, err:
        # Just print traceback
        print "something went wrong, here is some info:"
        traceback.print_exc()

        # Get traceback as a string and do something with it
        error = traceback.format_exc()
        print error.upper()

        # Log it through logging channel
        logging.error('Ooops', exc_info=True)
        sys.exit(-1)       
    
    
    
#python message_rabbit_reader.py localhost guest guest measures_quality_controlled localhost 8086 mydb 
if __name__ == "__main__":
    RABBIT_HOST = sys.argv[1] 
    RABBIT_USERNAME = sys.argv[2]
    RABBIT_PASSWORD = sys.argv[3]
    q_name = sys.argv[4] #measures_quality_controlled
    global INFLUX_HOST
    INFLUX_HOST = sys.argv[5]
    global INFLUX_PORT
    INFLUX_PORT = sys.argv[6]
    global INFLUX_DB
    INFLUX_DB = sys.argv[7]
    
	
    print('RABBIT_HOST: '+RABBIT_HOST+' RABBIT_USERNAME: '+RABBIT_PASSWORD+' q_name: '+q_name+' INFLUX_HOST: '+INFLUX_HOST+' INFLUX_PORT: '+INFLUX_PORT+' INFLUX_DB: '+INFLUX_DB)
    
    channel = init_chanel(RABBIT_HOST,q_name)
    channel.start_consuming()
        
    
    #init(INFLUX_DB,influx_base_url);
    
    




