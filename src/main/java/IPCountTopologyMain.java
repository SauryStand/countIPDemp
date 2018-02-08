import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolt.IPAnalysisCounter;
import woker.IPAnalysis;

/**
 * @Description:
 * @Created by Johnny Chou on 2/7/2018.
 * @Author：
 */
public class IPCountTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        // initial a topology
        TopologyBuilder builder = new TopologyBuilder();
        //for ip file abruption
        //builder.setSpout("IP-reader", new IPReader());
        //builder.setBolt("IP-normalizer", new IPCounter(),2).shuffleGrouping("IP-reader");
        //builder.setBolt("word-counter", new IPCounter(), 2).fieldsGrouping("word-reader", new Fields("word"));

        builder.setSpout("IP-Analysis", new IPAnalysis());
        builder.setBolt("IP-AnalysisCounter", new IPAnalysisCounter(),2).shuffleGrouping("IP-Analysis");

        Config config = new Config();
        //config.put("IPsFile", "h:/testing_folder/words.txt");
        config.put("IPsFile", "D:/Source_codes/My_java_projects/ipDivide/temp1/ip_sources.txt");
        config.setDebug(false);
        //提交topology
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        ////创建一个本地模式cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
