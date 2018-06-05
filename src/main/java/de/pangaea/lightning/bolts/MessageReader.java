/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import static de.pangaea.lightning.bolts.Constants.SUBSCRIPTIONURL;
import static de.pangaea.lightning.bolts.Constants.envriConsumer01;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MessageReader extends BaseRichSpout {

    private static final JsonNodeFactory factory = JsonNodeFactory.instance;
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpClient client;
    private static final long serialVersionUID = 1L;
    private String subscription = "";
    boolean _isDistributed;
    static SpoutOutputCollector _collector;

    public MessageReader() {
        this(true);
    }

    public MessageReader(String subscription) {
        this.subscription = subscription;
    }

    public MessageReader(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        client = HttpClientBuilder.create().build();
    }
    

    @Override
    public void close() {

    }

    public void ackMessage() {

    }

    @Override
    public void nextTuple() {
        System.out.println("PULLING MESSAGES...");
        //JsonNode message= new JsonNode();
        try {
            HttpPost request = new HttpPost(
                    SUBSCRIPTIONURL + this.getSubscription() + ":pull?key=" + envriConsumer01);
            StringEntity postJsonData = new StringEntity("{\"maxMessages\": \"1\"}");
            request.setHeader("Content-type", "application/json");
            request.setEntity(postJsonData);

            HttpResponse response = client.execute(request);
            if (response != null) {
                InputStream in = response.getEntity().getContent();
                //System.out.println(response.getStatusLine());
                JsonNode node = mapper.readTree(in);
                Iterator<JsonNode> messages = node.get("receivedMessages").elements();
                while (messages.hasNext()) {
                    JsonNode message = messages.next();
                    String ackId = message.get("ackId").asText();
                    _collector.emit(new Values(message));
                    HttpPost ackrequest = new HttpPost(
                            SUBSCRIPTIONURL + this.getSubscription() + ":acknowledge?key=" + envriConsumer01);
                    StringEntity ackJsonData = new StringEntity("{\"ackIds\":[\"" + ackId + "\"]}");
                    ackrequest.setHeader("Content-type", "application/json");
                    ackrequest.setEntity(ackJsonData);
                    HttpResponse ackresponse = client.execute(ackrequest);
                    // For some reasons the bolt has to do "something" otherwise the Spout hangs there
                    HttpEntity entity = ackresponse.getEntity();
                    String content = EntityUtils.toString(entity);
                    // System.out.println(content);									
                }

            }
        } catch (IOException | UnsupportedOperationException | ParseException ex) {
            // handle exception here
            System.out.println("ERROR: Could not execute nextTuple: " + ex.getMessage() + " ");
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("observationmessage"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (!_isDistributed) {
            Map<String, Object> ret = new HashMap<>();
            // ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }

    public String getSubscription() {
        return subscription;
    }
}
