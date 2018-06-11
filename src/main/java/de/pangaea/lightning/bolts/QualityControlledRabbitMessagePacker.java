/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.pangaea.lightning.Observation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
                //message+=" "+obs.getResultTime()+" "+obs.getResultValue()+" "+obs.getQualityOfObservation()+"\r\n";
            }
            //creating the message we'll send to EGI Argo
            String datamessage = "";
            String messagehead = "{\"messages\": [{\"attributes\":{\"madeBySensor\":\"" + madeBySensor + "\",\"observedProperty\":\"" + observedProperty + "\"},\"data\":\"";
            String jsonObs = mapper.writeValueAsString(packObs);
            byte[] encodedjsonObs = Base64.getEncoder().encode(jsonObs.getBytes());
            datamessage = new String(encodedjsonObs);

            message = messagehead + datamessage + "\"}]}";
            System.out.println("Sending Quality Controlled Message Package");
//            System.err.println(message);
            System.err.println(jsonObs);

        } catch (JsonProcessingException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(QualityControlledRabbitMessagePacker.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
