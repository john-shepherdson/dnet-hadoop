
package eu.dnetlib.dhp.common.collection;

import java.io.*;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.collection.models.CSVProgramme;
import eu.dnetlib.dhp.common.collection.models.CSVProject;
import eu.dnetlib.dhp.common.collection.models.DOAJModel;
import eu.dnetlib.dhp.common.collection.models.UnibiGoldModel;

public class GetCSVTest {

	private static String workingDir;

	private static LocalFileSystem fs;

	@Disabled
	@Test
	void getProgrammeFileTest() throws Exception {

		String fileURL = "https://cordis.europa.eu/data/reference/cordisref-h2020programmes.csv";

		GetCSV
				.getCsv(
						fs, new BufferedReader(
								new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL))),
						workingDir + "/programme",
						"eu.dnetlib.dhp.common.collection.models.CSVProgramme", ';');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			CSVProgramme csvp = new ObjectMapper().readValue(line, CSVProgramme.class);
			if (count == 0) {
				Assertions.assertTrue(csvp.getCode().equals("H2020-EU.5.f."));
				Assertions
						.assertTrue(
								csvp
										.getTitle()
										.startsWith(
												"Develop the governance for the advancement of responsible research and innovation by all stakeholders"));
				Assertions
						.assertTrue(csvp.getTitle().endsWith("promote an ethics framework for research and innovation"));
				Assertions.assertTrue(csvp.getShortTitle().equals(""));
				Assertions.assertTrue(csvp.getLanguage().equals("en"));
			}
			if (count == 28) {
				Assertions.assertTrue(csvp.getCode().equals("H2020-EU.3.5.4."));
				Assertions
						.assertTrue(
								csvp
										.getTitle()
										.equals(
												"Grundlagen für den Übergang zu einer umweltfreundlichen Wirtschaft und Gesellschaft durch Öko-Innovation"));
				Assertions
						.assertTrue(csvp.getShortTitle().equals("A green economy and society through eco-innovation"));
				Assertions.assertTrue(csvp.getLanguage().equals("de"));
			}
			if (count == 229) {
				Assertions.assertTrue(csvp.getCode().equals("H2020-EU.3.2."));
				Assertions
						.assertTrue(
								csvp
										.getTitle()
										.equals(
												"SOCIETAL CHALLENGES - Food security, sustainable agriculture and forestry, marine, maritime and inland water research, and the bioeconomy"));
				Assertions
						.assertTrue(
								csvp.getShortTitle().equals("Food, agriculture, forestry, marine research and bioeconomy"));
				Assertions.assertTrue(csvp.getLanguage().equals("en"));
			}
			Assertions.assertTrue(csvp.getCode() != null);
			Assertions.assertTrue(csvp.getCode().startsWith("H2020"));
			count += 1;
		}

		Assertions.assertEquals(767, count);
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(GetCSVTest.class.getSimpleName())
			.toString();

		fs = FileSystem.getLocal(new Configuration());
	}

	@Disabled
	@Test
	void getProjectFileTest() throws IOException, CollectorException, ClassNotFoundException {
		String fileURL = "https://cordis.europa.eu/data/cordis-h2020projects.csv";
		// String fileURL = "/Users/miriam.baglioni/Downloads/cordis-h2020projects.csv";

		GetCSV
			.getCsv(
				fs,
				new BufferedReader(new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL)))
				// new BufferedReader(new FileReader(fileURL))
				, workingDir + "/projects",
				"eu.dnetlib.dhp.common.collection.models.CSVProject", ';');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/projects"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			CSVProject csvp = new ObjectMapper().readValue(line, CSVProject.class);
			if (count == 0) {
				Assertions.assertTrue(csvp.getId().equals("771736"));
				Assertions.assertTrue(csvp.getProgramme().equals("H2020-EU.1.1."));
				Assertions.assertTrue(csvp.getTopics().equals("ERC-2017-COG"));

			}
			if (count == 22882) {
				Assertions.assertTrue(csvp.getId().equals("752903"));
				Assertions.assertTrue(csvp.getProgramme().equals("H2020-EU.1.3.2."));
				Assertions.assertTrue(csvp.getTopics().equals("MSCA-IF-2016"));
			}
			if (count == 223023) {
				Assertions.assertTrue(csvp.getId().equals("861952"));
				Assertions.assertTrue(csvp.getProgramme().equals("H2020-EU.4.e."));
				Assertions.assertTrue(csvp.getTopics().equals("SGA-SEWP-COST-2019"));
			}
			Assertions.assertTrue(csvp.getId() != null);
			Assertions.assertTrue(csvp.getProgramme().startsWith("H2020"));
			count += 1;
		}

		Assertions.assertEquals(34957, count);
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
				"eu.dnetlib.dhp.common.collection.models.UnibiGoldModel", ',');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			UnibiGoldModel unibi = new ObjectMapper().readValue(line, UnibiGoldModel.class);
			if (count == 0) {
				Assertions.assertTrue(unibi.getIssn().equals("0001-625X"));
				Assertions.assertTrue(unibi.getIssn_l().equals("0001-625X"));
				Assertions.assertTrue(unibi.getTitle().equals("Acta Mycologica"));

			}
			if (count == 43158) {
				Assertions.assertTrue(unibi.getIssn().equals("2088-6330"));
				Assertions.assertTrue(unibi.getIssn_l().equals("2088-6330"));
				Assertions.assertTrue(unibi.getTitle().equals("Religió: Jurnal Studi Agama-agama"));

			}
			if (count == 67027) {
				Assertions.assertTrue(unibi.getIssn().equals("2658-7068"));
				Assertions.assertTrue(unibi.getIssn_l().equals("2308-2488"));
				Assertions.assertTrue(unibi.getTitle().equals("Istoriko-èkonomičeskie issledovaniâ."));
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
				"eu.dnetlib.dhp.common.collection.models.DOAJModel", ',');

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
				Assertions.assertEquals("",doaj.getIssn());
				Assertions.assertEquals("2055-7159", doaj.getEissn());
				Assertions.assertEquals("BJR|case reports", doaj.getJournalTitle());
			}
			if (count == 16707) {

				Assertions.assertEquals("",doaj.getIssn());
				Assertions.assertEquals("2788-6298",doaj.getEissn());
				Assertions
					.assertEquals("Teacher Education through Flexible Learning in Africa", doaj.getJournalTitle());
			}

			count += 1;
		}

		Assertions.assertEquals(16713, count);
	}

}
