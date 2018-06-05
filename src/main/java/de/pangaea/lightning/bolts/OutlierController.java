/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pangaea.lightning.bolts;

import static de.pangaea.lightning.ENVRI_NRTQualityCheck.OutlierWindowSize;
import de.pangaea.lightning.Observation;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class OutlierController extends BaseWindowedBolt {
        //Robust Z-score: https://www.ibm.com/support/knowledgecenter/SSWLVY_1.0.0/com.ibm.spss.analyticcatalyst.help/analytic_catalyst/modified_z.html
        //Value is calculated for the middle value

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int windowSize = OutlierWindowSize;
            double MAD = 0;
            DescriptiveStatistics mad = new DescriptiveStatistics();
            List<Tuple> tuplesInWindow = inputWindow.get();
            int windowMid = (int) windowSize / 2;
            double[] valuesToCheck = new double[windowSize];
            double Zscore = 0;
            String observedPoperty = null;
            String madeBySensor = null;
            Observation middleobs = new Observation();
            int i = 0;
            if (tuplesInWindow.size() == windowSize) {
                for (Tuple tuple : tuplesInWindow) {
                    Observation obs = (Observation) tuple.getValue(0);
                    observedPoperty = (String) tuple.getValue(1);
                    madeBySensor = (String) tuple.getValue(2);
                    valuesToCheck[i] = (double) obs.getResultValue();
                    if (i == windowMid) {
                        middleobs = obs;
                    }
                    i++;
                }
                double[] absDeviationFromMedian = new double[valuesToCheck.length];
                Median m = new Median();
                double medianValue = m.evaluate(valuesToCheck);
                //calculating MAD
                for (int j = 0; j < valuesToCheck.length; j++) {
                    absDeviationFromMedian[j] = Math.abs(valuesToCheck[j] - medianValue);
                    mad.addValue(absDeviationFromMedian[j]);
                }
                MAD = m.evaluate(absDeviationFromMedian);
                double meanAD = mad.getMean();
                //calculating modified Z-score M
                if (MAD == 0) {
                    Zscore = Math.abs(valuesToCheck[windowMid] - medianValue) / 1.253314 * meanAD;
                } else {
                    Zscore = Math.abs(valuesToCheck[windowMid] - medianValue) / 1.486 * MAD;
                }
                //Zscore[j] =(0.6745*(valuesToCheck[j]- medianValue))/MAD;
                if (Zscore > 3.5) {
                    //System.out.println("Failed: "+valuesToCheck[middle]+" Z-Score: "+Zscore);							
                    middleobs.setQualityOfObservation(1);
                } else {
                    //System.out.println("Passed: "+valuesToCheck[middle]+" Z-Score: "+Zscore); 
                    if (middleobs.getQualityOfObservation() != 1) {
                        middleobs.setQualityOfObservation(0);
                    }
                }
                collector.emit(new Values(middleobs, observedPoperty, madeBySensor));
            } else {
                if (tuplesInWindow.size() < windowMid) {
                    Tuple lasttuple = tuplesInWindow.get(tuplesInWindow.size() - 1);
                    Observation lastobs = (Observation) lasttuple.getValue(0);
                    observedPoperty = (String) lasttuple.getValue(1);
                    madeBySensor = (String) lasttuple.getValue(2);
                    collector.emit(new Values(lastobs, observedPoperty, madeBySensor));
                    //System.out.println(" obs: "+(tuplesInWindow.size()-1)+" "+((Observation) tuplesInWindow.get(tuplesInWindow.size()-1).getValue(0)).getResultValue());
                }
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("zScoreCheckedObservation", "observedProperty", "madeBySensor"));
        }

    }
