package qfrag.computation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;

import qfrag.aggregation.AggregationStorage;
import qfrag.conf.Configuration;
import qfrag.conf.SparkConfiguration;
import qfrag.utils.Logging;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Created by ehussein on 10/17/17.
 */
public class QFragEngine extends Logging implements CommonExecutionEngine, Serializable {
    //////////// fields from level 0 /////////////
    public boolean computed = false;
    SparkConfiguration configuration = Configuration.get();

    //////////// fields from level 1 /////////////
    public int partitionId;
    public int superstep = 0;
    private Map<String, AccumulatorV2> accums = null;
    Broadcast previousAggregationsBc;
    public int numPartitionsPerWorker = configuration.numPartitionsPerWorker();
    //public AggregationStorageFactory aggregationStorageFactory = new AggregationStorageFactory();
    //public Map<String,AggregationStorage<? extends Writable, ? extends Writable>> aggregationStorages = null;
    public long filesLength = 0;
    private String inputFilePath = null;

    public QFragEngine(int _partitionId) {
        this.partitionId = _partitionId;
    }

    public QFragEngine(int _partitionId, int _superstep, Broadcast<String> _inputBC) {
        setLogLevel(configuration.getLogLevel());

        this.partitionId = _partitionId;
        this.superstep = _superstep;
        inputFilePath = _inputBC.getValue();
    }

    //////////// methods from level 0 /////////////
    @Override
    public int getPartitionId() { return partitionId;}

    @Override
    public int getNumberPartitions() { return configuration.numPartitions();}

    @Override
    public long getSuperstep() { return superstep;}
    //////////// End of methods from level 0 /////////////

    //////////// methods from level 1 /////////////
    public Iterator<Tuple2> flush() {
        return null;
    }

    public QFragEngine withNewAggregations(Broadcast aggregationsBc) {
        return null;
    }

    public void init() {
        logInfo("Partition(" + partitionId + ") is initializing.");
    }

    public void compute() {
        logInfo("I am partition(" + partitionId + "), and I received file path {" + inputFilePath + "} from the master.");
    }

    @Override
    public void finalize() {
        try {
            super.finalize();
        }
        catch (Throwable t) {
            logError(t.getMessage());
        }
    }

    private void flushStatsAccumulators() {
        logInfo("I am partition(" + partitionId + "), and my file_length = " + filesLength);
        accumulate(Long.valueOf(filesLength),accums.get(QFragMasterEngine.FILE_LENGTH));
    }

    private void accumulate(Long it,AccumulatorV2<Long,Long> accum) {
        accum.add(it);
    }

    public void flushAggregationsByName(String name) {
     }

    /**
     * Returns the current value of an aggregation installed in this execution
     * engine.
     *
     * @param name name of the aggregation
     * @return the aggregated value or null if no aggregation was found
     */
    public <A extends Writable> A getAggregatedValue(String name) {
        return null;
    }

    /**
     * Maps (key,value) to the respective local aggregator
     *
     * @param name identifies the aggregator
     * @param key key to account for
     * @param value value to be accounted for key in that aggregator
     *
     */
    public <K extends Writable, V extends Writable> void map(String name, K key, V value) {

    }

    /**
     * Retrieves or creates the local aggregator for the specified name.
     * Obs. the name must match to the aggregator's metadata configured in
     * *initAggregations* (Computation)
     *
     * @param name aggregator's name
     * @return an aggregation storage with the specified name
     */
    public <K extends Writable, V extends Writable> AggregationStorage<K,V> getAggregationStorage(String name, K key, V value) {

        return null;
    }

    public void aggregate(String name, LongWritable value) {
        AccumulatorV2<Long,Long> accum = accums.get(name);
        if(accum != null) {
            accum.add(value.get());
        }
        else
            logWarning("Aggregator/Accumulator " + name +" not found");
    }

/*    private <T extends ClassTag> void accumulate(T it,AccumulatorV2<T,T> accum) {
        accum.add(it);
    }*/

    //////////// End of methods from level 1 /////////////

}
