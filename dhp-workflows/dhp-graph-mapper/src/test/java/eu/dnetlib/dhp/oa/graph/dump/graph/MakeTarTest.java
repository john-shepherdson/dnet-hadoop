package eu.dnetlib.dhp.oa.graph.dump.graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

public class MakeTarTest {
    private static String workingDir;

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.graph.MakeTarTest.class.getSimpleName())
                .toString();
    }

    @Test
    public void testTar() throws IOException {
        LocalFileSystem fs = FileSystem.getLocal(new Configuration());

        fs
                .copyFromLocalFile(
                        false, new Path(getClass()
                                .getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
                                .getPath()),
                        new Path(workingDir + "/zenodo/ni/ni"));
        fs
                .copyFromLocalFile(
                        false, new Path(getClass()
                                .getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/dh-ch")
                                .getPath()),
                        new Path(workingDir + "/zenodo/dh-ch/dh-ch"));

    }
}
