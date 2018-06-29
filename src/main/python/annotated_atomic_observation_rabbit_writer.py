#!/usr/bin/env python
import pika
import sys
import datetime
import random
import base64




def send_observation(connection):
    now = datetime.datetime.now()
    date = (now.strftime('%m/%d/%Y'))
    message_data = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sos:InsertResult xmlns:sos=\"http://www.opengis.net/sos/2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" service=\"SOS\" version=\"2.0.0\" xsi:schemaLocation=\"http://www.opengis.net/sos/2.0 http://schemas.opengis.net/sos/2.0/sos.xsd\"><sos:template>http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml</sos:template><sos:resultValues>" + date + "#" + str(000631) + "#" + str(random.uniform(7, 9)) + "#" + str(random.uniform(20, 21)) + "#" + str(1) + "#" + str(random.uniform(0.0006419, 0.0007419))+ "#" + str(random.uniform(4, 5))+ "#" + str(random.uniform(50, 51))+ "#" + str(random.uniform(4, 5)) + "@</sos:resultValues></sos:InsertResult>";
    
    messageDataEnc = base64.b64encode(message_data)
    
    jsonString = "{\"attributes\":{\"madeBySensor\":\"http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml\",\"hasFeatureOfInterest\":\"http://example.org/features/1\",\"observedProperty\":\"http://purl.obolibrary.org/obo/PATO_0000146\"},\"data\":\"" + messageDataEnc + "\"}"

    
    channel.basic_publish(exchange='',
                        routing_key='measures',
                        body=jsonString,
                        properties=pika.BasicProperties(
                            delivery_mode = 2, # make message persistent
                        ))
    print("Sent %r" % jsonString)
    




if __name__ == "__main__":
    RABBIT_HOST = sys.argv[1]
    num_of_messages = int(sys.argv[2])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='measures', durable=True)
    
    for i in range(0,num_of_messages):
        send_observation(connection)
        
    connection.close()