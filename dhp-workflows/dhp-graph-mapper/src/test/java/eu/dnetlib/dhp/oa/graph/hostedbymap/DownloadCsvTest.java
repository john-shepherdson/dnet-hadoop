package eu.dnetlib.dhp.oa.graph.hostedbymap;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.GetCSV;
import eu.dnetlib.dhp.common.collection.HttpConnector2;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.DOAJModel;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.UnibiGoldModel;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DownloadCsvTest {

    private static String workingDir;

    private static LocalFileSystem fs;

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(DownloadCsvTest.class.getSimpleName())
                .toString();

        fs = FileSystem.getLocal(new Configuration());
    }

    @Disabled
    @Test
    void getUnibiFileTest() throws CollectorException, IOException, ClassNotFoundException {

        String fileURL = "https://pub.uni-bielefeld.de/download/2944717/2944718/issn_gold_oa_version_4.csv";

        GetCSV
                .getCsv(
                        fs, new BufferedReader(
                                new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL))),
                        workingDir + "/programme",
                        UnibiGoldModel.class.getName());

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

        String line;
        int count = 0;
        while ((line = in.readLine()) != null) {
            UnibiGoldModel unibi = new ObjectMapper().readValue(line, UnibiGoldModel.class);
            if (count == 0) {
                assertTrue(unibi.getIssn().equals("0001-625X"));
                assertTrue(unibi.getIssnL().equals("0001-625X"));
                assertTrue(unibi.getTitle().equals("Acta Mycologica"));

            }
            if (count == 43158) {
                assertTrue(unibi.getIssn().equals("2088-6330"));
                assertTrue(unibi.getIssnL().equals("2088-6330"));
                assertTrue(unibi.getTitle().equals("Religió: Jurnal Studi Agama-agama"));

            }
            if (count == 67027) {
                assertTrue(unibi.getIssn().equals("2658-7068"));
                assertTrue(unibi.getIssnL().equals("2308-2488"));
                assertTrue(unibi.getTitle().equals("Istoriko-èkonomičeskie issledovaniâ."));
            }

            count += 1;
        }

        Assertions.assertEquals(67028, count);
    }

    @Disabled
    @Test
    void getDoajFileTest() throws CollectorException, IOException, ClassNotFoundException {

        String fileURL = "https://doaj.org/csv";

        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL)))) {
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/DOAJ_1.csv")))) {
                String line;
                while ((line = in.readLine()) != null) {
                    writer.println(line.replace("\\\"", "\""));
                }
            }
        }

        GetCSV
                .getCsv(
                        fs, new BufferedReader(
                                new FileReader("/tmp/DOAJ_1.csv")),
                        workingDir + "/programme",
                        DOAJModel.class.getName());

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

        String line;
        int count = 0;
        while ((line = in.readLine()) != null) {
            DOAJModel doaj = new ObjectMapper().readValue(line, DOAJModel.class);
            if (count == 0) {
                Assertions.assertEquals("0001-3765", doaj.getIssn());
                Assertions.assertEquals("1678-2690", doaj.getEissn());
                Assertions.assertEquals("Anais da Academia Brasileira de Ciências", doaj.getJournalTitle());

            }
            if (count == 7904) {
                System.out.println(new ObjectMapper().writeValueAsString(doaj));
                Assertions.assertEquals("", doaj.getIssn());
                Assertions.assertEquals("2055-7159", doaj.getEissn());
                Assertions.assertEquals("BJR|case reports", doaj.getJournalTitle());
            }
            if (count == 16707) {

                Assertions.assertEquals("", doaj.getIssn());
                Assertions.assertEquals("2788-6298", doaj.getEissn());
                Assertions
                        .assertEquals("Teacher Education through Flexible Learning in Africa", doaj.getJournalTitle());
            }

            count += 1;
        }

        Assertions.assertEquals(16713, count);
    }

    @AfterAll
    public static void cleanup() {
        FileUtils.deleteQuietly(new File(workingDir));
    }

}
