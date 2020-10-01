
package eu.dnetlib.dhp.actionmanager.project;


import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.actionmanager.project.utils.CSVParser;

public class CSVParserTest {


	@Test
	public void readProgrammeTest() throws Exception {

		String programmecsv = IOUtils
			.toString(
				getClass()
					.getClassLoader()
					.getResourceAsStream("eu/dnetlib/dhp/actionmanager/project/programme.csv"));

		CSVParser csvParser = new CSVParser();

		List<Object> pl = csvParser.parse(programmecsv, "eu.dnetlib.dhp.actionmanager.project.utils.CSVProgramme");

		Assertions.assertEquals(24, pl.size());

	}

}
