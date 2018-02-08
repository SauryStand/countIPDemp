package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Description:
 * @Created by Johnny Chou on 2/8/2018.
 * @Author：
 */
public class IPAnalysisCounter implements IRichBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector collector;
    Map<String, Integer> tmpMap = new HashMap<String, Integer>();
    Map<String, Integer> map = new HashMap<String, Integer>();
    Map<String, Integer> finalMap = new HashMap<String, Integer>();
    static int max = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.id = topologyContext.getThisTaskId();
        this.name = topologyContext.getThisComponentId();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String ipStr = tuple.getString(0);
        if (tmpMap.containsKey(ipStr)) {
            int count = tmpMap.get(ipStr);
            tmpMap.put(ipStr, count + 1);
        } else {
            tmpMap.put(ipStr, 0);
        }
        countIpQuantities(tmpMap,map);//in this round, we can only count single file
        tmpMap.clear();

        //countIpQuantities(map,finalMap);
        Iterator<String> it = finalMap.keySet().iterator();
//        while(it.hasNext()){
//            String ip = it.next();
//            //System.out.println("result IP : " + ip + " | count = " + finalMap.get(ip));
////            if(max == Integer.valueOf(finalMap.get(ip))){
////                System.out.println("result IP : " + ip + " | max count = " + finalMap.get(ip));
////            }
//        }
        // 确认成功处理一个tuple
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * return a final map
     * @param pMap
     * @param resultMap
     */
    private static void countIpQuantities(Map<String, Integer> pMap, Map<String, Integer> resultMap) {
        Iterator<Map.Entry<String, Integer>> it = pMap.entrySet().iterator();

        String resultIp = "";
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
            if (entry.getValue() > max) {
                max = entry.getValue();
                resultIp = entry.getKey();
            }
        }
        resultMap.put(resultIp,max);
    }



}
