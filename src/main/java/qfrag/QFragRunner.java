package qfrag; /**
 * Created by ehussein on 10/16/17.
 */

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import qfrag.conf.YamlConfiguration;
import qfrag.conf.SparkConfiguration;
import qfrag.computation.QFragMasterEngine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import qfrag.utils.Logging;
import scala.collection.JavaConversions;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;

import java.io.IOException;

public class QFragRunner extends Logging implements Tool {
    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(QFragRunner.class);
    /**
     * Writable conf
     */
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        logInfo("Args are:\n");
        for(String s: args)
            logInfo(s);
        logInfo("");

        YamlConfiguration yamlConfig = new YamlConfiguration(args);
        yamlConfig.load();

        LOG.info ("execution engine " + yamlConfig.getExecutionEngine());

        if (yamlConfig.isSparkExecutionEngine())
            return runSpark (yamlConfig);
        else return 1;
    }

    private int runSpark(YamlConfiguration yamlConfig) throws Exception {
        SparkConfiguration config = new SparkConfiguration (JavaConversions.mapAsScalaMap(yamlConfig.getProperties()));
        JavaSparkContext sc = new JavaSparkContext(config.sparkConf());

        System.out.println("Communication strategy in CONFIG= " + config.getCommStrategy());

        QFragMasterEngine masterEngine = new QFragMasterEngine(sc, config);

        masterEngine.compute();
        masterEngine.finalizeComputation();
        return 0;
    }

    protected GiraphJob getJob(GiraphConfiguration conf, String jobName)
            throws IOException {
        return new GiraphJob(conf, jobName);
    }

    /**
     * Execute ArabesqueRunner.
     *
     * @param args Typically command line arguments.
     * @throws Exception Any exceptions thrown.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new QFragRunner(), args));
    }
}
