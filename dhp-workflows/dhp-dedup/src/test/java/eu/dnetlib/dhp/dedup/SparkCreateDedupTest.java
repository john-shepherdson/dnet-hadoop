package eu.dnetlib.dhp.dedup;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class SparkCreateDedupTest {

    String configuration;
    String entity = "organization";

    @Before
    public void setUp() throws IOException {
//        configuration = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dedup/conf/org.curr.conf.json"));
        configuration = "";
    }

    @Test
    @Ignore
    public void createSimRelsTest() throws Exception {
        SparkCreateSimRels.main(new String[]{
                "-mt", "local[*]",
                "-i", "/Users/miconis/dumps",
                "-o", "/tmp/dedup/rawset_test",
                "-asi", "dedup-similarity-result-levenstein",
                "-la", "lookupurl",
                "-w", "workingPath"
        });
    }

    @Test
    @Ignore
    public void createCCTest() throws Exception {

        SparkCreateConnectedComponent.main(new String[]{
                "-mt", "local[*]",
                "-s", "/Users/miconis/dumps",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void dedupRecordTest() throws Exception {
        SparkCreateDedupRecord.main(new String[]{
                "-mt", "local[*]",
                "-s", "/Users/miconis/dumps",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-d", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void printConfiguration() throws Exception {
        System.out.println(ArgumentApplicationParser.compressArgument(configuration));
    }

    @Test
    @Ignore
    public void testHashCode() {
        final String s1 = "20|grid________::6031f94bef015a37783268ec1e75f17f";
        final String s2 = "20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46";

        final HashFunction hashFunction = Hashing.murmur3_128();

        System.out.println(s1.hashCode());
        System.out.println(hashFunction.hashString(s1).asLong());
        System.out.println(s2.hashCode());
        System.out.println(hashFunction.hashString(s2).asLong());
    }

    @Test
    public void fileExistsTest() throws IOException {

        boolean result = false;

        FileSystem fileSystem = FileSystem.get(new Configuration());

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/tmp"));

        for (FileStatus fs : fileStatuses) {
            if (fs.isDirectory()) {
                if (fileSystem.exists(new Path(DedupUtility.createMergeRelPath("/tmp", fs.getPath().getName(), "cicciopasticcio")))) {
                    System.out.println("fs = " + DedupUtility.createMergeRelPath("/tmp", fs.getPath().getName(), "cicciopasticcio"));
                    result = true;
                }
            }
        }

    }
}
