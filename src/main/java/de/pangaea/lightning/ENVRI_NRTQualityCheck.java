/*
 * Copyright (c) PANGAEA - Data Publisher for Earth & Environmental Science
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.pangaea.lightning;

import de.pangaea.lightning.bolts.RabbitMessageReader;
import de.pangaea.lightning.bolts.MessageAtomizer;
import de.pangaea.lightning.bolts.OutlierController;
import de.pangaea.lightning.bolts.QualityControlledRabbitMessagePacker;
import de.pangaea.lightning.bolts.RangeCheckController;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;

/**
 * <p>
 * Title: BasicExample
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Project:
 * </p>
 * <p>
 * Copyright: PANGAEA
 * </p>
 */
public class ENVRI_NRTQualityCheck {

    public static int OutlierWindowSize = 50;

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("MessageReader", new RabbitMessageReader("envri_sub_101"), 1);
        builder.setBolt("MessageAtomizer", new MessageAtomizer(), 1).fieldsGrouping("MessageReader", new Fields("observationmessage"));
        builder.setBolt("RangeCheckController", new RangeCheckController(), 1).fieldsGrouping("MessageAtomizer", new Fields("observedProperty"));
        //will fail if more than 20 parameters are submitted in parallel because of grouping -> number of working nodes=20
        builder.setBolt("OutlierController", new OutlierController().withWindow(new Count(OutlierWindowSize), new Count(1)), 1)
                .fieldsGrouping("RangeCheckController", new Fields("observedProperty"));
        builder.setBolt("QualityControlledMessagePacker", new QualityControlledRabbitMessagePacker()
                .withTumblingWindow(new Count(20)), 1)
                .fieldsGrouping("OutlierController", new Fields("observedProperty"));
        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 10000);
        conf.setSkipMissingKryoRegistrations(false);
        conf.setMaxSpoutPending(5000);
        conf.setStatsSampleRate(1.0d);
        if (args != null && args.length > 0) {
            try {
                conf.setNumWorkers(1);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException ex) {
                Logger.getLogger(ENVRI_NRTQualityCheck.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ENVRI_NRTQualityCheck", conf, builder.createTopology());
        }
    }
}
