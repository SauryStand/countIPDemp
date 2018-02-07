import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.WordCounter;
import bolt.WordNormalizer;
import woker.WordReader;

/**
 * @Description:
 * @Created by Johnny Chou on 2/6/2018.
 * @Author：
 */
public class WordCountTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        // initial a topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
        Config config = new Config();
        config.put("wordsFile", "h:/testing_folder/words.txt");
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
