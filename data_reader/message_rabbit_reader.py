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


def init_chanel(queue_name,rabbitmq_host):    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=q_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=q_name)
    return channel


    
def write_to_influx(observations):
    influx_base_url = 'http://'+INFLUX_HOST+':'+INFLUX_PORT
    create_db_influx_url = influx_base_url+'/query?q=CREATE DATABASE '+INFLUX_DB
    r = requests.post(create_db_influx_url)
    data_string = ""
    for observation in observations:
        sensor = observation['madeBySensor']    
        unit = observation['resultUnit']    
        quality = observation['qualityOfObservation']    
        value = observation['resultValue']    
        time = observation['resultTime']
        millis = int(round(float(time) * 1000))
        data_string += 'observations,sensor='+sensor+',unit='+unit+',quality='+str(quality)+' value='+str(value) +' '+str(millis)+'\n'
        print data_string

    r = requests.post(influx_base_url+'/write?consistency=one&precision=ms&db='+INFLUX_DB, data=data_string)
    print r
    
    
def on_request(ch, method, props, body):
    handle_delivery(body)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)       
    
    
    
def handle_delivery(message):
    parsed_json_message = json.loads(message)
    messages = parsed_json_message['messages']
    all_observations = []
    for message in messages:
        observations = json.loads(base64.b64decode(message['data']).decode('utf-8', 'ignore'))
        for observation in observations:
            all_observations.append(observation)
        
    write_to_influx(all_observations)
    
        

       
        
    
def callback(ch, method, properties, body):
    handle_delivery(body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    
    
    
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
    
	
    print('RABBIT_HOST: '+RABBIT_HOST+' RABBIT_USERNAME: '+RABBIT_PASSWORD+' RABBIT_VHOST: '+INFLUX_HOST+' INFLUX_PORT: '+INFLUX_DB)
    
    channel = init_chanel(RABBIT_HOST,q_name)
    channel.start_consuming()
        
    
    #init(INFLUX_DB,influx_base_url);
    
    




