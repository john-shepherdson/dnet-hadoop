
package eu.dnetlib.dhp.actionmanager.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProgramme;
import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProject;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.GetCSV;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

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
	void getProgrammeFileTest() throws Exception {

		String fileURL = "https://cordis.europa.eu/data/reference/cordisref-h2020programmes.csv";

		GetCSV
			.getCsv(
				fs, new BufferedReader(
					new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL))),
				workingDir + "/programme",
				CSVProgramme.class.getName(), ';');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			CSVProgramme csvp = new ObjectMapper().readValue(line, CSVProgramme.class);
			if (count == 0) {
				assertTrue(csvp.getCode().equals("H2020-EU.5.f."));
				assertTrue(
					csvp
						.getTitle()
						.startsWith(
							"Develop the governance for the advancement of responsible research and innovation by all stakeholders"));
				assertTrue(csvp.getTitle().endsWith("promote an ethics framework for research and innovation"));
				assertTrue(csvp.getShortTitle().equals(""));
				assertTrue(csvp.getLanguage().equals("en"));
			}
			if (count == 28) {
				assertTrue(csvp.getCode().equals("H2020-EU.3.5.4."));
				assertTrue(
					csvp
						.getTitle()
						.equals(
							"Grundlagen für den Übergang zu einer umweltfreundlichen Wirtschaft und Gesellschaft durch Öko-Innovation"));
				assertTrue(csvp.getShortTitle().equals("A green economy and society through eco-innovation"));
				assertTrue(csvp.getLanguage().equals("de"));
			}
			if (count == 229) {
				assertTrue(csvp.getCode().equals("H2020-EU.3.2."));
				assertTrue(
					csvp
						.getTitle()
						.equals(
							"SOCIETAL CHALLENGES - Food security, sustainable agriculture and forestry, marine, maritime and inland water research, and the bioeconomy"));
				assertTrue(
					csvp.getShortTitle().equals("Food, agriculture, forestry, marine research and bioeconomy"));
				assertTrue(csvp.getLanguage().equals("en"));
			}
			assertTrue(csvp.getCode() != null);
			assertTrue(csvp.getCode().startsWith("H2020"));
			count += 1;
		}

		assertEquals(767, count);
	}

	@Disabled
	@Test
	void getProjectFileTest() throws IOException, CollectorException, ClassNotFoundException {
		String fileURL = "https://cordis.europa.eu/data/cordis-h2020projects.csv";

		GetCSV
			.getCsv(
				fs,
				new BufferedReader(new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL))),
				workingDir + "/projects",
				CSVProject.class.getName(), ';');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/projects"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			CSVProject csvp = new ObjectMapper().readValue(line, CSVProject.class);
			if (count == 0) {
				assertTrue(csvp.getId().equals("771736"));
				assertTrue(csvp.getProgramme().equals("H2020-EU.1.1."));
				assertTrue(csvp.getTopics().equals("ERC-2017-COG"));

			}
			if (count == 22882) {
				assertTrue(csvp.getId().equals("752903"));
				assertTrue(csvp.getProgramme().equals("H2020-EU.1.3.2."));
				assertTrue(csvp.getTopics().equals("MSCA-IF-2016"));
			}
			if (count == 223023) {
				assertTrue(csvp.getId().equals("861952"));
				assertTrue(csvp.getProgramme().equals("H2020-EU.4.e."));
				assertTrue(csvp.getTopics().equals("SGA-SEWP-COST-2019"));
			}
			assertTrue(csvp.getId() != null);
			assertTrue(csvp.getProgramme().startsWith("H2020"));
			count += 1;
		}

		assertEquals(34957, count);
	}

	@AfterAll
	public static void cleanup() {
		FileUtils.deleteQuietly(new File(workingDir));
	}

	@Test
	void getLocalProgrammeFileTest() throws Exception {

		GetCSV
			.getCsv(
				fs, new BufferedReader(
					new FileReader(
						getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/h2020_programme.csv").getPath())),
				workingDir + "/programme",
				CSVProgramme.class.getName(), ';');

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(workingDir + "/programme"))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			CSVProgramme csvp = new ObjectMapper().readValue(line, CSVProgramme.class);
			if (count == 528) {
				assertEquals("H2020-EU.5.f.", csvp.getCode());
				assertTrue(
					csvp
						.getTitle()
						.startsWith(
							"Develop the governance for the advancement of responsible research and innovation by all stakeholders"));
				assertTrue(csvp.getTitle().endsWith("promote an ethics framework for research and innovation"));
				assertTrue(csvp.getShortTitle().equals(""));
				assertTrue(csvp.getLanguage().equals("en"));
			}
			if (count == 11) {
				assertEquals("H2020-EU.3.5.4.", csvp.getCode());
				assertTrue(
					csvp
						.getTitle()
						.equals(
							"Grundlagen für den Übergang zu einer umweltfreundlichen Wirtschaft und Gesellschaft durch Öko-Innovation"));
				assertTrue(csvp.getShortTitle().equals("A green economy and society through eco-innovation"));
				assertTrue(csvp.getLanguage().equals("de"));
			}
			if (count == 34) {
				assertTrue(csvp.getCode().equals("H2020-EU.3.2."));
				assertTrue(
					csvp
						.getTitle()
						.equals(
							"SOCIETAL CHALLENGES - Food security, sustainable agriculture and forestry, marine, maritime and inland water research, and the bioeconomy"));
				assertTrue(
					csvp.getShortTitle().equals("Food, agriculture, forestry, marine research and bioeconomy"));
				assertTrue(csvp.getLanguage().equals("en"));
			}
			assertTrue(csvp.getCode() != null);
			assertTrue(csvp.getCode().startsWith("H2020"));
			count += 1;
		}

		assertEquals(769, count);
	}

}
