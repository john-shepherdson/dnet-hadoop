
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.DOAJModel;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.UnibiGoldModel;

public class DownloadCsvTest {

	private static final Logger log = LoggerFactory.getLogger(DownloadCsvTest.class);

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

		final String outputFile = workingDir + "/unibi_gold.json";
		new DownloadCSV()
			.doDownload(
				fileURL,
				workingDir + "/unibi_gold",
				outputFile,
				UnibiGoldModel.class.getName(),
				',',
				fs);

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(outputFile))));

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

		assertEquals(67028, count);
	}

	@Disabled
	@Test
	void getDoajFileTest() throws CollectorException, IOException, ClassNotFoundException {

		String fileURL = "https://doaj.org/csv";

		final String outputFile = workingDir + "/doaj.json";
		new DownloadCSV()
			.doDownload(
				fileURL,
				workingDir + "/doaj",
				outputFile,
				DOAJModel.class.getName(),
				',',
				fs);

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(outputFile))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			DOAJModel doaj = new ObjectMapper().readValue(line, DOAJModel.class);
			if (count == 0) {
				assertEquals("0001-3765", doaj.getIssn());
				assertEquals("1678-2690", doaj.getEissn());
				assertEquals("Anais da Academia Brasileira de Ciências", doaj.getJournalTitle());
			}
			if (count == 22) {
				log.info(new ObjectMapper().writeValueAsString(doaj));
				System.out.println(new ObjectMapper().writeValueAsString(doaj));
			}
			if (count == 7904) {
				// log.info(new ObjectMapper().writeValueAsString(doaj));
				assertEquals("", doaj.getIssn());
				assertEquals("2055-7159", doaj.getEissn());
				assertEquals("BJR|case reports", doaj.getJournalTitle());
			}
			if (count == 16707) {

				assertEquals("2783-1043", doaj.getIssn());
				assertEquals("2783-1051", doaj.getEissn());
				assertEquals("فیزیک کاربردی ایران", doaj.getJournalTitle());
			}

			count += 1;
		}

		assertEquals(16715, count);
	}

	@AfterAll
	public static void cleanup() {
		FileUtils.deleteQuietly(new File(workingDir));
	}

}