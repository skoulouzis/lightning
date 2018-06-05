/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.pangaea.lightning.Observation;
import static de.pangaea.lightning.bolts.Constants.TOPICURL;
import static de.pangaea.lightning.bolts.Constants.envriPublisher01;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class QualityControlledMessagePacker extends BaseWindowedBolt {

    private OutputCollector collector;
    private HttpClient client;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        client = HttpClientBuilder.create().build();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        String message = "";
        ArrayList<Observation> packObs = new ArrayList();
        List<Tuple> tuplesInWindow = inputWindow.get();
        String observedProperty = "";
        String madeBySensor = "";
        for (Tuple tuple : tuplesInWindow) {
            Observation obs = (Observation) tuple.getValue(0);
            packObs.add(obs);
            observedProperty = (String) tuple.getValue(1);
            madeBySensor = (String) tuple.getValue(2);
            //message+=" "+obs.getResultTime()+" "+obs.getResultValue()+" "+obs.getQualityOfObservation()+"\r\n";
        }
        ObjectMapper mapper = new ObjectMapper();
        //creating the message we'll send to EGI Argo
        String datamessage = "";
        String messagehead = "{\"messages\": [{\"attributes\":{\"madeBySensor\":\"" + madeBySensor + "\",\"observedProperty\":\"" + observedProperty + "\"},\"data\":\"";
        try {
            String jsonObs = mapper.writeValueAsString(packObs);
            byte[] encodedjsonObs = Base64.getEncoder().encode(jsonObs.getBytes());
            datamessage = new String(encodedjsonObs);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        message = messagehead + datamessage + "\"}]}";
        System.out.println("Sending Quality Controlled Message Package");
        //System.out.println(TOPICURL+"envri_topic_101_qc:publish?key="+envriPublisher01);
        HttpPost request = new HttpPost(
                TOPICURL + "envri_topic_101_qc:publish?key=" + envriPublisher01);
        StringEntity postJsonData = null;
        try {
            postJsonData = new StringEntity(message);
        } catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        request.setHeader("Content-type", "application/json");
        request.setEntity(postJsonData);

        try {
            HttpResponse response = client.execute(request);

            if (response != null) {
                HttpEntity entity = response.getEntity();
                System.out.println(response.getStatusLine());
                String content = EntityUtils.toString(entity);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //

    }
}
