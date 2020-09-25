
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVParser;
import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme;

public class CSVParserTest {

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CSVParserTest.class.getSimpleName());

	}

	@Test
	public void readProgrammeTest() throws Exception {

		String programmecsv = IOUtils
			.toString(
				getClass()
					.getClassLoader()
					.getResourceAsStream("eu/dnetlib/dhp/actionmanager/project/programme.csv"));

		CSVParser csvParser = new CSVParser();

		List<Object> pl = csvParser.parse(programmecsv, "eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme");

		System.out.println(pl.size());

	}
}
