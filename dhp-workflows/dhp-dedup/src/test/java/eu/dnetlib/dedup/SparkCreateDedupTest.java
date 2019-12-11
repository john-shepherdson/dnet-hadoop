package eu.dnetlib.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SparkCreateDedupTest {

    String configuration;
    String entity = "organization";

    @Before
    public void setUp() throws IOException {
        configuration = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dedup/conf/org.curr.conf.json"));

    }

    @Test
    @Ignore
    public void createSimRelsTest() throws Exception {
        SparkCreateSimRels.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void createCCTest() throws Exception {

        SparkCreateConnectedComponent.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void dedupRecordTest() throws Exception {
        SparkCreateDedupRecord.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-d", "/tmp/dedup",
        });
    }

    @Test
    public void printCC() throws Exception {
        System.out.println(ArgumentApplicationParser.compressArgument(configuration));
    }



}
