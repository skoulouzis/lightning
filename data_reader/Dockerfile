FROM python:2.7.15-alpine3.7

ADD message_rabbit_reader.py /

RUN pip install requests

RUN chmod +x /message_rabbit_reader.py

CMD python message_rabbit_reader.py $RABBIT_HOST $RABBIT_PORT $RABBIT_USERNAME $RABBIT_PASSWORD $QUEUE_NAME $INFLUX_HOST $INFLUX_PORT $INFLUX_DB



# # measures_quality_controlled
