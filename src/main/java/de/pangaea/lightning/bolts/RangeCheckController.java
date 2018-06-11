/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.pangaea.lightning.Observation;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RangeCheckController extends BaseRichBolt {

    OutputCollector _collector;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            //        Observation rangeCheckedObservation = (Observation) tuple.getValue(0);
            String jsonObservation = (String) tuple.getValue(0);
            Observation rangeCheckedObservation = mapper.readValue(jsonObservation, Observation.class);
            float[] range = (float[]) tuple.getValue(1);
            float value = rangeCheckedObservation.getResultValue();
            if (range[0] < range[1]) {
                if (value >= range[0] && value <= range[1]) {
                    //System.out.println("Passed: "+value+" ("+range[0]+" - "+range[1]+")");
                    if (rangeCheckedObservation.getQualityOfObservation() != 1) {
                        rangeCheckedObservation.setQualityOfObservation(0);
                    }
                } else {
                    //System.out.println("Failed: "+value);
                    rangeCheckedObservation.setQualityOfObservation(1);
                }
            }
            Object observedProperty = tuple.getValue(2);
            Object madeBySensor = tuple.getValue(3);
            String jsonRangeCheckedObservation = mapper.writeValueAsString(rangeCheckedObservation);
            _collector.emit(new Values(jsonRangeCheckedObservation, observedProperty, madeBySensor));
            _collector.ack(tuple);
        } catch (IOException ex) {
            Logger.getLogger(RangeCheckController.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rangeCheckedObservation", "observedProperty", "madeBySensor"));
    }

}
