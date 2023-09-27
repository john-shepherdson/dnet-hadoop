package eu.dnetlib.dhp.swh;

import eu.dnetlib.dhp.swh.utils.SWHUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ArchiveRepositoryURLsTest {

    @Test
    void testArchive() throws IOException {
        String inputPath = getClass()
                .getResource("/eu/dnetlib/dhp/swh/lastVisitDataToArchive.csv")
                .getPath();

        File file = new File(inputPath);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream

        String line;
        while((line = br.readLine()) != null) {
            String[] tokens = line.split("\t");

            String response = ArchiveRepositoryURLs.handleRecord(tokens[0], tokens[1], 365);
            System.out.println(tokens[0] + "\t" + response);
            System.out.println();
        }
        fr.close();
    }
}
