package eu.dnetlib.dedup;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SparkCreateDedupTest {



    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/pub_dedup_vertex"));
        FileUtils.deleteDirectory(new File("/tmp/pub_dedup_rels"));
    }



    @Test
    @Ignore
    public void dedupTest() throws Exception {
        final String configuration = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dedup/conf/org.curr.conf.json"));


        SparkCreateSimRels.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", "publication",
                "-c", configuration,
                "-t", "/tmp/dedup",
        });

        SparkCreateConnectedComponent.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", "publication",
                "-c", configuration,
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void dedupRecordTest() throws Exception {
        SparkCreateDedupRecord.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", "publication",
                "-c", "configuration",
                "-t", "/tmp/dedup",
        });
    }




}
