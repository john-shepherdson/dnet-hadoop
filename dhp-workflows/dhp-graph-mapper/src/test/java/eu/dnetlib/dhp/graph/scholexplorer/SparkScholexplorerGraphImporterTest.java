package eu.dnetlib.dhp.graph.scholexplorer;

import org.junit.Test;

public class SparkScholexplorerGraphImporterTest {

    @Test

    public void testImport() throws Exception {
        SparkScholexplorerGraphImporter.main(new String[]{
                "-mt", "local[*]",
                "-e", "publication",
                "-s", "file:///data/scholexplorer_dump/pmf.dli.seq",
                "-t", "file:///data/scholexplorer_dump/pmf_dli_with_rel"}
        );


    }
}
