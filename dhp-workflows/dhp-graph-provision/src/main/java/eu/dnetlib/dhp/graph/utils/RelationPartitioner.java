package eu.dnetlib.dhp.graph.utils;

import eu.dnetlib.dhp.graph.model.SortableRelationKey;
import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;

/**
 * Used in combination with SortableRelationKey, allows to partition the records by source id, therefore
 * allowing to sort relations sharing the same source id by the ordering defined in SortableRelationKey.
 */
public class RelationPartitioner extends Partitioner {

    private int numPartitions;

    public RelationPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return Utils.nonNegativeMod(((SortableRelationKey) key).getSourceId().hashCode(), numPartitions());
    }

}
