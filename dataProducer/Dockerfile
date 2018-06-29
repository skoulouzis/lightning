FROM python:2.7.15-alpine3.7

ADD scripts/reportRabbitMQ2InfluxDB.py /

RUN pip install requests

RUN chmod +x /reportRabbitMQ2InfluxDB.py

CMD python /reportRabbitMQ2InfluxDB.py $RABBIT_HOST $RABBIT_PORT $RABBIT_USERNAME $RABBIT_PASSWORD $INFLUX_HOST $INFLUX_PORT $INFLUX_DB


# docker run -i -t -P -e RABBIT_HOST=172.17.0.5 -e RABBIT_PORT=15672 -e RABBIT_USERNAME=guest -e RABBIT_PASSWORD=guest -e RABBIT_QNAME=ckan_Mapping62.x3ml -e RABBIT_VHOST=%2F -e INFLUX_HOST=172.17.0.3 -e INFLUX_PORT=8086 -e INFLUX_DB=mydb rabbit-reporter 
