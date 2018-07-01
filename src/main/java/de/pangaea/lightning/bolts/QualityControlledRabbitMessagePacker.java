/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import de.pangaea.lightning.Observation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class QualityControlledRabbitMessagePacker extends BaseWindowedBolt {

    private OutputCollector collector;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String taskQeueName = "measures_quality_controlled";
    private ConnectionFactory factory;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        factory = new ConnectionFactory();

        String rabbitMQHost = System.getenv("RABBIT_HOST");
        if (rabbitMQHost == null) {
            rabbitMQHost = "localhost";
        }
        factory.setHost(rabbitMQHost);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(taskQeueName, true, false, false, null);

        } catch (IOException | TimeoutException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void execute(TupleWindow inputWindow) {
        try {
            String message;
            ArrayList<Observation> packObs = new ArrayList();
            List<Tuple> tuplesInWindow = inputWindow.get();
            String observedProperty = "";
            String madeBySensor = "";
            for (Tuple tuple : tuplesInWindow) {
//                Observation obs = (Observation) tuple.getValue(0);
                String jsonObs = (String) tuple.getValue(0);
                Observation obs = mapper.readValue(jsonObs, Observation.class);
                packObs.add(obs);
                observedProperty = (String) tuple.getValue(1);
                madeBySensor = (String) tuple.getValue(2);
                collector.ack(tuple);
                //message+=" "+obs.getResultTime()+" "+obs.getResultValue()+" "+obs.getQualityOfObservation()+"\r\n";
            }
            String datamessage;

            String messagehead = "{\"messages\": [{\"attributes\":{\"madeBySensor\":\"" + 
                    madeBySensor + "\",\"observedProperty\":\"" + observedProperty + "\"},\"data\":\"";
            String jsonObs = mapper.writeValueAsString(packObs);
            byte[] encodedjsonObs = Base64.getEncoder().encode(jsonObs.getBytes());
            datamessage = new String(encodedjsonObs);

            message = messagehead + datamessage + "\"}]}";
            sendMessage(message);

        } catch (JsonProcessingException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void sendMessage(String message) throws IOException {

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {

            channel.basicPublish("", taskQeueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));

        } catch (TimeoutException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
