/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RabbitMessageReader extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private String subscription = "";
    boolean _isDistributed;
    static SpoutOutputCollector _collector;

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
    }

    @Override
    public void close() {

    }

    public void ackMessage() {

    }

    @Override
    public void nextTuple() {
//            System.out.println("PULLING MESSAGES...");
//            String jsonString = "{"
//                    + "  \"messages\": ["
//                    + "  {"
//                    + "    \"attributes\":"
//                    + "    {"
//                    + "      \"madeBySensor\":\"http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml\","
//                    + "      \"hasFeatureOfInterest\":\"http://example.org/features/1\","
//                    + "      \"type\":\"AtomicObservation\","
//                    + "      \"observedProperty\":\"http://purl.obolibrary.org/obo/PATO_0000146\""
//                    + "    },"
//                    + "    \"data\":\"ew0KCSJpZCI6ICJodHRwOi8vZXhhbXBsZS5vcmcvb2JzZXJ2YXRpb25zLzEiLA0KICAgICAgICAibWFkZUJ5U2Vuc29yIjogImh0dHA6Ly9leGFtcGxlLm9yZy9zZW5zb3JzLzEiLA0KICAgICAgICAiaGFzRmVhdHVyZU9mSW50ZXJlc3QiOiAiaHR0cDovL2V4YW1wbGUub3JnL2ZlYXR1cmVzLzEiLA0KCSJvYnNlcnZlZFByb3BlcnR5IjogImh0dHA6Ly9leGFtcGxlLm9yZy9wcm9wZXJ0aWVzLzEiLA0KCSJyZXN1bHRUaW1lIjogIjIwMTctMDEtMDFUMDA6MDA6MDAuMDAwKzAyOjAwIiwNCiAgICAgICAgImhhc1Jlc3VsdCI6IHsNCgkgICAgImlkIjogImh0dHA6Ly9leGFtcGxlLm9yZy9yZXN1bHRzLzEiLA0KICAgICAgICAgICAgInVuaXQiOiAiaHR0cDovL3F1ZHQub3JnL3ZvY2FiL3VuaXQjRGVncmVlQ2Vsc2l1cyIsDQogICAgICAgICAgICAibnVtZXJpY1ZhbHVlIjogMTIuNQ0KCX0NCn0NCg==\""
//                    + "  }"
//                    + "  ]"
//                    + "}";
        String jsonString = "{"
                + "    \"attributes\":"
                + "    {"
                + "      \"madeBySensor\":\"http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml\","
                + "      \"hasFeatureOfInterest\":\"http://example.org/features/1\","
                + "      \"observedProperty\":\"http://purl.obolibrary.org/obo/PATO_0000146\""
                + "    },"
                + "    \"data\":\"PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c29zOkluc2VydFJlc3VsdCB4bWxuczpzb3M9Imh0dHA6Ly93d3cub3Blbmdpcy5uZXQvc29zLzIuMCIgeG1sbnM6eHNpPSJodHRwOi8vd3d3LnczLm9yZy8yMDAxL1hNTFNjaGVtYS1pbnN0YW5jZSIgc2VydmljZT0iU09TIiB2ZXJzaW9uPSIyLjAuMCIgeHNpOnNjaGVtYUxvY2F0aW9uPSJodHRwOi8vd3d3Lm9wZW5naXMubmV0L3Nvcy8yLjAgaHR0cDovL3NjaGVtYXMub3Blbmdpcy5uZXQvc29zLzIuMC9zb3MueHNkIj48c29zOnRlbXBsYXRlPmh0dHA6Ly9kYXRhcG9ydGFscy5wYW5nYWVhLmRlL3NtbC9kYi9wdHViZS9zc3dfNTllOWE4MTYxY2ZiMi54bWw8L3Nvczp0ZW1wbGF0ZT4gICAgPHNvczpyZXN1bHRWYWx1ZXM+MjItMDItMTdfMDI6NTE6MjgjMDAwNjMxIzguMDY1ODg4NSMyMS40NDM2MDAwIzM2LjA3MzkzMzEjMC4wMDA2NDE5IzQuODUwMDAwMCM1MC43MzM5MDAwIzQuNzQ5ODIxN0A8L3NvczpyZXN1bHRWYWx1ZXM+PC9zb3M6SW5zZXJ0UmVzdWx0Pg==\""
                + "}";
//            JsonNode node = mapper.readTree(jsonString);

        _collector.emit(new Values(jsonString));

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
