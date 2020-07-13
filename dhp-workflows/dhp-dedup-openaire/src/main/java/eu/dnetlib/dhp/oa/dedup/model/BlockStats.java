package eu.dnetlib.dhp.oa.dedup.model;

public class BlockStats {

    private String key; //key of the block
    private Long size; //number of elements in the block
    private Long comparisons; //number of comparisons in the block

    public BlockStats() {
    }

    public BlockStats(String key, Long size, Long comparisons) {
        this.key = key;
        this.size = size;
        this.comparisons = comparisons;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public Long getComparisons() {
        return comparisons;
    }

    public void setComparisons(Long comparisons) {
        this.comparisons = comparisons;
    }

}
