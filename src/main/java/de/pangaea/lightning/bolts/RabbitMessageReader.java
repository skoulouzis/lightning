/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RabbitMessageReader extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private String subscription = "";
    boolean _isDistributed;
    static SpoutOutputCollector _collector;
    private Date start;
    int messageLen = 20;
    String pattern = "d-MM-YY_HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

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
    public void nextTuple() {
        Random r = new Random();
        for (int i = 0; i < messageLen; i++) {
            Date now = new Date();
            String dateStr = simpleDateFormat.format(now);
            String measurement = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sos:InsertResult xmlns:sos=\"http://www.opengis.net/sos/2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" service=\"SOS\" version=\"2.0.0\" xsi:schemaLocation=\"http://www.opengis.net/sos/2.0 http://schemas.opengis.net/sos/2.0/sos.xsd\"><sos:template>http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml</sos:template><sos:resultValues>"
                    + dateStr + "#" + 000631 + "#"
                    + (7 + (9 - 0) * r.nextDouble()) + "#"
                    + (20 + (21 - 0) * r.nextDouble()) + "#"
                    + 1 + "#"
                    + (0.0006419 + (0.0007419 - 0) * r.nextDouble()) + "#"
                    + (4 + (5 - 0) * r.nextDouble()) + "#"
                    + (50 + (51 - 0) * r.nextDouble()) + "#"
                    + (4 + (5 - 0) * r.nextDouble())
                    + "@</sos:resultValues></sos:InsertResult>";

            String messageDataEnc = new String(Base64.getEncoder().encode(measurement.getBytes()));

            String jsonString = "{"
                    + "    \"attributes\":"
                    + "    {"
                    + "      \"madeBySensor\":\"http://dataportals.pangaea.de/sml/db/ptube/ssw_59e9a8161cfb2.xml\","
                    + "      \"hasFeatureOfInterest\":\"http://example.org/features/1\","
                    + "      \"observedProperty\":\"http://purl.obolibrary.org/obo/PATO_0000146\""
                    + "    },"
                    + "    \"data\":\"" + messageDataEnc + "\""
                    + "}";

            _collector.emit(new Values(jsonString), jsonString.hashCode());
            this.ack(jsonString.hashCode());
//            Utils.sleep(sleepTime);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("observationmessage"));
    }
}
