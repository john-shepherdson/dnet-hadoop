
package eu.dnetlib.dhp.oa.provision.utils;

import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;

/**
 * Used in combination with SortableRelationKey, allows to partition the records by source id, therefore allowing to
 * sort relations sharing the same source id by the ordering defined in SortableRelationKey.
 */
public class RelationPartitioner extends Partitioner {

	private final int numPartitions;

	public RelationPartitioner(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	@Override
	public int numPartitions() {
		return numPartitions;
	}

	@Override
	public int getPartition(Object key) {
		String partitionKey = (String) key;
		return Utils.nonNegativeMod(partitionKey.hashCode(), numPartitions());
	}

}
