
package eu.dnetlib.dhp.doiboost;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.CSVParser;

public class GetCSVTest {

	@Test
	public void readUnibiGoldTest() throws Exception {

		String programmecsv = IOUtils
			.toString(
				getClass()
					.getClassLoader()
					.getResourceAsStream("eu/dnetlib/dhp/doiboost/issn_gold_oa_version_4.csv"));

		CSVParser csvParser = new CSVParser();

		List<Object> pl = csvParser.parse(programmecsv, "eu.dnetlib.doiboost.UnibiGoldModel", ',');

		Assertions.assertEquals(72, pl.size());

//        ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//
//        pl.forEach(res -> {
//            try {
//                System.out.println(OBJECT_MAPPER.writeValueAsString(res));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//        });

	}
}
