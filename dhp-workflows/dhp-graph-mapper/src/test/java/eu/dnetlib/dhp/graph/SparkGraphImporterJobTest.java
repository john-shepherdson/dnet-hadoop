package eu.dnetlib.dhp.graph;

import org.junit.Test;

public class SparkGraphImporterJobTest {

    @Test
    public void  testImport() throws Exception {
        SparkGraphImporterJob.main(new String[]{"-mt", "local[*]","-i", "/home/sandro/part-m-02236", "-o", "/tmp/dataframes"});
    }

}
