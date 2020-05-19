package eu.dnetlib.dhp.actionmanager.project;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReadCSVTest {

    private static Path workingDir;

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(eu.dnetlib.dhp.actionmanager.project.ReadCSVTest.class.getSimpleName());


    }
        @Test
    public  void readProgrammeTest() throws Exception {

        String programmecsv = IOUtils.toString(getClass()
                .getClassLoader().getResourceAsStream("eu/dnetlib/dhp/actionmanager/project/programme.csv"));
        ReadCSV
                .main(
                        new String[] {
                                "-fileURL",
                                "http://cordis.europa.eu/data/reference/cordisref-H2020programmes.csv",
                                "-outputPath",
                                workingDir.toString() + "/project",
                                "-hdfsPath",
                                getClass().getResource("/eu/dnetlib/dhp/blacklist/blacklist").getPath(),
                                "-mergesPath",
                                getClass().getResource("/eu/dnetlib/dhp/blacklist/mergesRelOneMerge").getPath(),
                        });




    }
}
