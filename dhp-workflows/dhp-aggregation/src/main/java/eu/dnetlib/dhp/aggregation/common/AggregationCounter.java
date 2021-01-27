package eu.dnetlib.dhp.aggregation.common;

import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;


public class AggregationCounter implements Serializable {
    private LongAccumulator totalItems;
    private LongAccumulator errorItems;
    private LongAccumulator processedItems;

    public AggregationCounter() {
    }

    public AggregationCounter(LongAccumulator totalItems, LongAccumulator errorItems, LongAccumulator processedItems) {
        this.totalItems = totalItems;
        this.errorItems = errorItems;
        this.processedItems = processedItems;
    }

    public LongAccumulator getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(LongAccumulator totalItems) {
        this.totalItems = totalItems;
    }

    public LongAccumulator getErrorItems() {
        return errorItems;
    }

    public void setErrorItems(LongAccumulator errorItems) {
        this.errorItems = errorItems;
    }

    public LongAccumulator getProcessedItems() {
        return processedItems;
    }

    public void setProcessedItems(LongAccumulator processedItems) {
        this.processedItems = processedItems;
    }
}
