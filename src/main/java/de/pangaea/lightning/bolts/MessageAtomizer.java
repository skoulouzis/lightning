/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.pangaea.lightning.HasResult;
import de.pangaea.lightning.InsertResultMessage;
import de.pangaea.lightning.Observation;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.xml.sax.SAXException;

/**
 *
 * @author S. Koulouzis
 */
public class MessageAtomizer extends BaseRichBolt {

    OutputCollector _collector;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
//        JsonNode node = (JsonNode) tuple.getValueByField("observationmessage");
            String jsonString = (String) tuple.getValueByField("observationmessage");
            JsonNode mess = mapper.readTree(jsonString);
            JsonNode messageAttributes = mess.get("attributes");

//        String messageType = messageAttributes.get("type").asText();
            String madeBySensor = messageAttributes.get("madeBySensor").asText();
            String observedProperty = messageAttributes.get("observedProperty").asText();
            String featureOfInterest = messageAttributes.get("hasFeatureOfInterest").asText();

            String messageData = mess.get("data").asText();
            String messageDataDecoded = new String(Base64.getDecoder().decode(messageData));

            try {
                InsertResultMessage insertMessage;
                String unit = "";
                float[] allowedRange = {0, 0};
                SensorMetadata sensor = new SensorMetadata(madeBySensor);
                for (int s = 0; s < sensor.observedProperties.size(); s++) {
                    if (sensor.observedProperties.get(s).id.equals(observedProperty)) {
                        unit = sensor.observedProperties.get(s).unit;
                        allowedRange = sensor.observedProperties.get(s).getMeasurementRange();
                    }
                }

                insertMessage = new InsertResultMessage(messageDataDecoded);
                for (Map.Entry<String, String> entry : insertMessage.resultValues.entrySet()) {
                    float resultValue = Float.parseFloat(entry.getValue());
                    String resultTime = entry.getKey();
//                    Observation observation = new Observation(madeBySensor, observedProperty, new HasResult(resultValue));
                    Observation observation = new Observation();
                    observation.setMadeBySensor(madeBySensor);
                    observation.setResultValue(resultValue);
                    observation.setObservedProperty(observedProperty);
                    HasResult hasResult = new HasResult();
                    hasResult.setNumericValue(resultValue);
                    observation.setHasResult(hasResult);
                    observation.setResultTime(resultTime);
                    observation.setResultUnit(unit);
                    observation.setFeatureOfInterest(featureOfInterest);
                    String jsonObservation = mapper.writeValueAsString(observation);
                    _collector.emit(new Values(jsonObservation, allowedRange, observedProperty, madeBySensor));
                }
                _collector.ack(tuple);
            } catch (ParserConfigurationException | SAXException | IOException ex) {
                Logger.getLogger(MessageAtomizer.class.getName()).log(Level.SEVERE, null, ex);
            }

        } catch (IOException ex) {
            Logger.getLogger(MessageAtomizer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("observation", "allowedrange", "observedProperty", "madeBySensor"));
    }

}
