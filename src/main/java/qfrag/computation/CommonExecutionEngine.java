package qfrag.computation;

import qfrag.aggregation.AggregationStorage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public interface CommonExecutionEngine {

    <A extends Writable> A getAggregatedValue(String name);
    
    //<K extends Writable, V extends Writable> AggregationStorage<K, V> getAggregationStorage(String name);

    //<K extends Writable, V extends Writable> void map(String name, K key, V value);
    
    int getPartitionId();

    int getNumberPartitions();

    long getSuperstep();

    void aggregate(String name, LongWritable value);
    
    //void output(String outputString);
}
