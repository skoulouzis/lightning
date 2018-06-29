/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.rabbitmq.client.AMQP;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMessageReader extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private String subscription = "";
    boolean _isDistributed;
    static SpoutOutputCollector _collector;
    private Date start;
    int messageLen = 20;
    int counter = 0;
    String pattern = "d-MM-YY_HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    private ConnectionFactory factory;
    private String taskQeueName = "measures";

    public RabbitMessageReader() {
        this(true);
    }

    public RabbitMessageReader(String subscription) {
        this.subscription = subscription;
    }

    public RabbitMessageReader(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        factory = new ConnectionFactory();

        String rabbitMQHost = System.getenv("RABBIT_HOST");
        if (rabbitMQHost == null) {
            rabbitMQHost = "localhost";
        }
        factory.setHost(rabbitMQHost);
    }

    @Override
    public void nextTuple() {
        try {
            final Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(taskQeueName, true, false, false, null);
            channel.basicQos(1);
            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                    String jsonString = new String(body, "UTF-8");
                    try {
                        _collector.emit(new Values(jsonString), jsonString.hashCode());
                        ack(jsonString.hashCode());

                    } finally {
                        if (channel.isOpen()) {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    }
                }
            };
            channel.basicConsume(taskQeueName, false, consumer);

        } catch (IOException | TimeoutException ex) {
            Logger.getLogger(RabbitMessageReader.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("observationmessage"));
    }
}
