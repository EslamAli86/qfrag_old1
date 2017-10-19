package qfrag.computation;

import org.apache.hadoop.io.Writable;
import qfrag.utils.Logging;

public abstract class CommonMasterExecutionEngine extends Logging {

    abstract int getSuperstep();

    abstract void haltComputation();

    abstract public <A extends Writable> A getAggregatedValue(String name);

    abstract public <A extends Writable> void setAggregatedValue(String name, A value);

}
