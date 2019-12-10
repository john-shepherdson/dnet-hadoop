package eu.dnetlib.dedup;

import org.apache.spark.Partitioner;

import java.io.Serializable;

public class OafPartitioner extends Partitioner implements Serializable {

    private final int numPartitions;

    public OafPartitioner(int partitions) {
        assert (partitions > 0);
        this.numPartitions = partitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        if (key instanceof OafKey) {
            @SuppressWarnings("unchecked")
            OafKey item = (OafKey) key;
            return Math.abs(item.getDedupId().hashCode() % numPartitions);
        } else {
            throw new IllegalArgumentException("Unexpected Key");
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + numPartitions;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof OafPartitioner)) {
            return false;
        }
        //
        OafPartitioner other = (OafPartitioner) obj;
        if (numPartitions != other.numPartitions) {
            return false;
        }
        //
        return true;
    }
}
