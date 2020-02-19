package eu.dnetlib.dhp.graph;

import org.junit.Test;

public class ImportDataFromMongoTest {

    @Test
    public void doTest() throws Exception {
        ImportDataFromMongo.main(new String[] {
                "-h", "localhost",
                "-p", "2800",
                "-f", "PMF",
                "-l", "store",
                "-i", "cleaned",
                "-dn", "mdstore_dli",
                "-n", "file:///home/sandro/test.seq",
                "-u", "sandro",
                "-t", "file:///home/sandro/test.seq"
                });
    }

}
