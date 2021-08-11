
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.actionmanager.project.utils.EXCELParser;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.HttpConnector2;

@Disabled
public class EXCELParserTest {

	private static Path workingDir;
	private HttpConnector2 httpConnector = new HttpConnector2();
	private static final String URL = "https://cordis.europa.eu/data/reference/cordisref-h2020topics.xlsx";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CSVParserTest.class.getSimpleName());

	}

	@Test
	void test1() throws CollectorException, IOException, InvalidFormatException, ClassNotFoundException,
		IllegalAccessException, InstantiationException {

		EXCELParser excelParser = new EXCELParser();

		List<Object> pl = excelParser
			.parse(
				httpConnector.getInputSourceAsStream(URL), "eu.dnetlib.dhp.actionmanager.project.utils.EXCELTopic",
				"Topics");

		Assertions.assertEquals(3878, pl.size());

	}
}
