package qfrag.computation;

// hadoop imports
import org.apache.hadoop.io.Writable;
// spark imports
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

// java imports
import java.io.Serializable;
import java.util.*;
// qfrag imports
import qfrag.conf.SparkConfiguration;
import qfrag.aggregation.AggregationStorage;

import scala.Array;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions.*;

/**
 * Created by ehussein on 10/16/17.
 */
public class QFragMasterEngine extends CommonMasterExecutionEngine implements Serializable {
    private SparkConfiguration config = null;
    private JavaSparkContext sc = null;
    private int numPartitions = 0;
    private int superstep = 0;

    // Spark accumulators for stats counting (non-critical)
    // Ad-hoc approach for user-defined aggregations
    private Map<String, AccumulatorV2> aggAccums = null;
    private Map<String, AggregationStorage> aggregations = null;
    // Accums names
    final public static String FILE_LENGTH = "file_length";
    private String inputFilePath = "";
    private Broadcast<String> inputBC = null;
    private Broadcast<SparkConfiguration> configBC;

    JavaRDD globalRDD = null;
    ComputationFunction computeFunction = null;

    public QFragMasterEngine(JavaSparkContext _sc, SparkConfiguration _config) {
        this.config = _config;
        config.initialize();
        this.sc = _sc;

        init();
    }

    public QFragMasterEngine(Map<String, Object> confs) {
        this.config = new SparkConfiguration (JavaConversions.mapAsScalaMap(confs));
        config.initialize();
        this.sc = new JavaSparkContext(config.sparkConf());

        init();
    }

    public QFragMasterEngine(SparkConfiguration _config) {
        this.config = _config;
        config.initialize();
        this.sc = new JavaSparkContext(config.sparkConf());

        init();
    }

    public void init() {
        String log_level = config.getLogLevel();
        logInfo("Setting log level to " + log_level);
        setLogLevel(log_level);
        sc.setLogLevel(log_level.toUpperCase());
        config.setIfUnset ("num_partitions", sc.defaultParallelism());
        config.setHadoopConfig (sc.hadoopConfiguration());
        numPartitions = config.numPartitions();

        inputBC = sc.broadcast(inputFilePath);
        configBC = sc.broadcast(config);

        globalRDD = sc.parallelize(Arrays.asList(), numPartitions).cache();
    }

    public void compute() {
        JavaRDD<QFragEngine> execEngines = getExecutionEngines(configBC,0);

        execEngines.persist (StorageLevel.MEMORY_ONLY());
        execEngines.foreachPartition(new VoidFunction<Iterator<QFragEngine>>() {
            @Override
            public void call(Iterator<QFragEngine> qFragEngineIterator) throws Exception {

            }
        });
    }

    public void finalizeComputation() {
        try {
            super.finalize();
        }
        catch (Throwable t) {
            logError(t.getMessage());
        }
    }

    public int getNumberPartitions() { return numPartitions; }

    public Map<String,AggregationStorage<? extends Writable,? extends Writable>> mergeOrReplaceAggregations
            (Map<String,AggregationStorage<? extends Writable,? extends Writable>> aggreagtions,
             Map<String,AggregationStorage<? extends Writable,? extends Writable>> previousAggregations) {

        if(config.isAggregationIncremental()) {
            for(Map.Entry<String,AggregationStorage<? extends Writable,? extends Writable>> entry: previousAggregations.entrySet()) {
                aggreagtions.put(entry.getKey(), entry.getValue());
            }
            return aggreagtions;
        }
        else
            return previousAggregations;
    }

    private void getAggregations(JavaRDD<QFragEngine> execEngines, int numPartitions) {
        /*
        List x = new ArrayList();
        JavaConversions.asScalaBuffer(x).toList();
        sc.parallelize(JavaConversions.asScalaBuffer(x).toList(),numPartitions);
        */
    }

    private void checkSerializable(Object obj) {
        if(!(obj instanceof Serializable))
            logInfo("\n\n" + obj.getClass().getName() + " is not serializable\n");
        else
            logInfo("\n\n" + obj.getClass().getName() + " is serializable\n");

        if(!(Serializable.class.isInstance(obj)))
            logInfo("\n\n" + obj.getClass().getName() + " is not serializable\n");
        else
            logInfo("\n\n" + obj.getClass().getName() + " is serializable\n");
    }

    private void checkNull(Object obj) {
        if(obj != null)
            logInfo("\n\n" + obj.getClass().getName() + " is not null\n");
        else
            logInfo("\n\n" + obj.getClass().getName() + " is null\n");
    }

    private JavaRDD<QFragEngine> getExecutionEngines(final Broadcast<SparkConfiguration> configBc,
                                                     int _superstep ) {
        configBc.value().initialize();
        computeFunction = new ComputationFunction();//inputBC,configBc,superstep);

        globalRDD = sc.parallelize(new ArrayList<QFragEngine>(), numPartitions).cache();


        checkNull(globalRDD);
        checkSerializable(globalRDD);

        checkNull(computeFunction);
        checkSerializable(computeFunction);

        JavaRDD<QFragEngine> executors = globalRDD.mapPartitionsWithIndex(computeFunction,false);


        return executors;
    }

    //////////// Overridden methods from CommonMasterExecutionEngine /////////////
    @Override
    public void haltComputation() {
        logInfo("Halting Master Computation");
        sc.stop();
    }

    @Override
    public int getSuperstep() { return superstep; }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregationStorage aggStorage = aggregations.get(name);

        if(aggStorage == null) {
            logWarning ("AggregationStorage " + name + " not found");
            return null;
        }
        else
            return (A) aggStorage;
    }

    @Override
    public <A extends Writable> void setAggregatedValue(String name, A value) {
        logWarning ("Setting aggregated value has no effect in spark execution engine");
    }


}

class ComputationFunction implements Function2<Integer, Iterator<QFragEngine>, Iterator<QFragEngine>>, Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
    /*        private static final long serialVersionUID = 7526472295622776147L;
            private int superstep;
            private Broadcast<String> inBC = null;
            private Broadcast<SparkConfiguration> confBC;
    */
    public ComputationFunction() {

    }
    /*
            public ComputationFunction(Broadcast<String> _inBC, Broadcast<SparkConfiguration> _confBC, int _superstep) {
                this.inBC = _inBC;
                this.confBC = _confBC;
                this.superstep = _superstep;
            }
    */
    @Override
    public Iterator<QFragEngine> call(Integer partitionId, Iterator<QFragEngine> v2) throws Exception {
        //confBC.value().initialize();
        QFragEngine engine = new QFragEngine(partitionId);//, this.superstep, inBC);

        engine.init();
        engine.compute();
        engine.finalize();

        ArrayList<QFragEngine> list = new ArrayList<QFragEngine>();
        list.add(engine);
        return list.iterator();
    }
}
