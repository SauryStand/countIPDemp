package woker;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Created by Johnny Chou on 2/7/2018.
 * @Author：
 */
public class IPReader implements IRichSpout {

    //protected final static int HASH_NUM = 1000;

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    public boolean isDistributed(){return false;}

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        try{
            //获取创建Topology时指定的要读取的文件路径
            this.fileReader = new FileReader(map.get("IPsFile").toString());
        }catch(FileNotFoundException e){
            throw new RuntimeException("Error reading file [" + map.get("IPsFile") + "]");
        }
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if(completed){
            try{
                Thread.sleep(1000);
            }catch(InterruptedException e){

            }
            return;
        }
        String ip;
        Map<String, StringBuilder> map  = new HashMap<String,StringBuilder>();
        try {
            // Read all lines
            BufferedReader reader = new BufferedReader(fileReader);
            while ((ip = reader.readLine()) != null) {
                //String hashIp = modifyToHash(ip);
                //发射每一行，Values是一个ArrayList的实现
                this.collector.emit(new Values(ip), ip);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {
        System.out.println("OK:" + o);
    }

    @Override
    public void fail(Object o) {
        System.out.println("FAIL:" + o);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
