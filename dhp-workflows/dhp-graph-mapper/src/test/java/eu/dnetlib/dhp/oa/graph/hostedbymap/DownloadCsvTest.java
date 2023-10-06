
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

		String fileURL = "https://pub.uni-bielefeld.de/download/2944717/2944718/issn_gold_oa_version_5.csv";

		final String outputFile = workingDir + "/unibi_gold.json";
		new DownloadCSV()
			.doDownload(
				fileURL,
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

	@AfterAll
	public static void cleanup() {
		FileUtils.deleteQuietly(new File(workingDir));
	}

}
