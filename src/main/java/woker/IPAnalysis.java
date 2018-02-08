package woker;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @Description: analysis the quantity of IP
 * @Created by Johnny Chou on 2/8/2018.
 * @Author：
 */
public class IPAnalysis implements IRichSpout {

    protected final static String FOLDER = "D:/Source_codes/My_java_projects/ipDivide/temp1/file_set/";
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    /**
     * inner class, it works for calculating the IP quantities
     */
    class MatchFileUtil implements Callable<String>{

        @Override
        public String call() throws Exception {
            return null;
        }
    }




    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        File folder = new File(FOLDER);
        File[] files = folder.listFiles();
        System.out.println("---------->>>file's quanlity is "+files.length);
        for (File file : files) {
            try{
                this.fileReader =  new FileReader(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        this.collector = spoutOutputCollector;
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
                //发射每一行，Values是一个ArrayList的实现
                this.collector.emit(new Values(ip), ip);
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }

    }


    @Override
    public void close() {
        try {
            this.fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    @Override
    public void ack(Object o) {
        System.out.println("---------->>>ack receive data is OK:" + o.toString());
    }

    @Override
    public void fail(Object o) {
        System.out.println("---------->>>FAIL:" + o);
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
