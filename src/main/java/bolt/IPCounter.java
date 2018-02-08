package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Description:
 * @Created by Johnny Chou on 2/7/2018.
 * @Author：
 */
public class IPCounter implements IRichBolt {

    protected final static String FOLDER = "D:/Source_codes/My_java_projects/ipDivide/temp1/file_set2/";
    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector collector;
    int count = 0;
    int tempCount = 0;
    Map<String, StringBuilder> map  = new HashMap<String,StringBuilder>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        String ipStr = tuple.getString(0);
        //File asd = (File) tuple.getValue(0);
        String hashIp = modifyToHash(ipStr);

        if (map.containsKey(hashIp)) {
            StringBuilder sb = (StringBuilder)map.get(hashIp);
            sb.append(ipStr).append("\n");
            map.put(hashIp,sb);
        } else {
            StringBuilder sb = new StringBuilder(ipStr);
            sb.append("\n");
            map.put(hashIp,sb);
        }
        count++;//be careful of this point, it might has bugs
        if(count == 20){
            try{
                Iterator<String> it = map.keySet().iterator();
                File ipFile = new File(FOLDER + "/" + tempCount + ".txt");
                FileWriter fileWriter = new FileWriter(ipFile, true);
                while(it.hasNext()){
                    String fileName = it.next();
                    StringBuilder sb = map.get(fileName);
                    fileWriter.write(sb.toString());
                    tempCount++;
                }
                fileWriter.close();
                count = 0;
                map.clear();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 确认成功处理一个tuple
        collector.ack(tuple);
    }

    private static String modifyToHash(String ip) {
        long numIp = ipToLong(ip);
        return String.valueOf(numIp % 1000);
    }

    /**
     * 在这里采取的是就是Hash取模的方式，将字符串的ip地址给转换成一个长整数，并将这个数对1000取模，
     * 将模一样的ip放到同一个文件，这样就能够生成1000个小文件，每个文件就只有1M多，在这里已经是足够小的了。
     * @param strIp
     * @return
     */
    private static long ipToLong(String strIp) {
        long[] ip = new long[4];
        int position1 = strIp.indexOf(".");
        int position2 = strIp.indexOf(".", position1 + 1);
        int position3 = strIp.indexOf(".", position2 + 1);

        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
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



}

