package eu.dnetlib.dhp.graph.scholexplorer;

import org.junit.Ignore;
import org.junit.Test;

public class SparkScholexplorerMergeEntitiesJobTest {

    @Test
    @Ignore
    public void testMerge() throws Exception {
        SparkScholexplorerMergeEntitiesJob.main(new String[]{
                "-mt", "local[*]",
                "-e", "relation",
                "-s", "file:///Users/sandro/Downloads/scholix/relation",
                "-t", "file:///Users/sandro/Downloads/scholix/relation"}
        );
    }
}
