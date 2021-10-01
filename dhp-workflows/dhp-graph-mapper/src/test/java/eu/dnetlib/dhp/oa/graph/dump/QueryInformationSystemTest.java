
package eu.dnetlib.dhp.oa.graph.dump;

import static org.mockito.Mockito.lenient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.dom4j.DocumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xml.sax.SAXException;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
class QueryInformationSystemTest {

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		" and ($x//context/param[./@name = 'status']/text() = 'all') "
		+
		"  return " +
		"<community> " +
		"{$x//CONFIGURATION/context/@id}" +
		"{$x//CONFIGURATION/context/@label}" +
		"</community>";

	List<String> communityMap = Arrays
		.asList(
			"<community id=\"egi\" label=\"EGI Federation\"/>",
			"<community id=\"fet-fp7\" label=\"FET FP7\"/>",
			"<community id=\"fet-h2020\" label=\"FET H2020\"/>",
			"<community id=\"clarin\" label=\"CLARIN\"/>",
			"<community id=\"rda\" label=\"Research Data Alliance\"/>",
			"<community id=\"ee\" label=\"SDSN - Greece\"/>",
			"<community id=\"dh-ch\" label=\"Digital Humanities and Cultural Heritage\"/>",
			"<community id=\"fam\" label=\"Fisheries and Aquaculture Management\"/>",
			"<community id=\"ni\" label=\"Neuroinformatics\"/>",
			"<community id=\"mes\" label=\"European Marine Science\"/>",
			"<community id=\"instruct\" label=\"Instruct-ERIC\"/>",
			"<community id=\"elixir-gr\" label=\"ELIXIR GR\"/>",
			"<community id=\"aginfra\" label=\"Agricultural and Food Sciences\"/>",
			"<community id=\"dariah\" label=\"DARIAH EU\"/>",
			"<community id=\"risis\" label=\"RISIS\"/>",
			"<community id=\"epos\" label=\"EPOS\"/>",
			"<community id=\"beopen\" label=\"Transport Research\"/>",
			"<community id=\"euromarine\" label=\"EuroMarine\"/>",
			"<community id=\"ifremer\" label=\"Ifremer\"/>",
			"<community id=\"oa-pg\" label=\"EC Post-Grant Open Access Pilot\"/>",
			"<community id=\"science-innovation-policy\" label=\"Science and Innovation Policy Studies\"/>",
			"<community id=\"covid-19\" label=\"COVID-19\"/>",
			"<community id=\"enermaps\" label=\"Energy Research\"/>");

	@Mock
	private ISLookUpService isLookUpService;

	private QueryInformationSystem queryInformationSystem;

	private Map<String, String> map;

	@BeforeEach
	public void setUp() throws ISLookUpException, DocumentException, SAXException {
		lenient().when(isLookUpService.quickSearchProfile(XQUERY)).thenReturn(communityMap);
		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(isLookUpService);
		map = queryInformationSystem.getCommunityMap(false, null);
	}

	@Test
	void testSize() throws ISLookUpException {

		Assertions.assertEquals(23, map.size());
	}

	@Test
	void testContent() {
		Assertions.assertTrue(map.containsKey("egi") && map.get("egi").equals("EGI Federation"));

		Assertions.assertTrue(map.containsKey("fet-fp7") && map.get("fet-fp7").equals("FET FP7"));
		Assertions.assertTrue(map.containsKey("fet-h2020") && map.get("fet-h2020").equals("FET H2020"));
		Assertions.assertTrue(map.containsKey("clarin") && map.get("clarin").equals("CLARIN"));
		Assertions.assertTrue(map.containsKey("rda") && map.get("rda").equals("Research Data Alliance"));
		Assertions.assertTrue(map.containsKey("ee") && map.get("ee").equals("SDSN - Greece"));
		Assertions
			.assertTrue(
				map.containsKey("dh-ch") && map.get("dh-ch").equals("Digital Humanities and Cultural Heritage"));
		Assertions.assertTrue(map.containsKey("fam") && map.get("fam").equals("Fisheries and Aquaculture Management"));
		Assertions.assertTrue(map.containsKey("ni") && map.get("ni").equals("Neuroinformatics"));
		Assertions.assertTrue(map.containsKey("mes") && map.get("mes").equals("European Marine Science"));
		Assertions.assertTrue(map.containsKey("instruct") && map.get("instruct").equals("Instruct-ERIC"));
		Assertions.assertTrue(map.containsKey("elixir-gr") && map.get("elixir-gr").equals("ELIXIR GR"));
		Assertions
			.assertTrue(map.containsKey("aginfra") && map.get("aginfra").equals("Agricultural and Food Sciences"));
		Assertions.assertTrue(map.containsKey("dariah") && map.get("dariah").equals("DARIAH EU"));
		Assertions.assertTrue(map.containsKey("risis") && map.get("risis").equals("RISIS"));
		Assertions.assertTrue(map.containsKey("epos") && map.get("epos").equals("EPOS"));
		Assertions.assertTrue(map.containsKey("beopen") && map.get("beopen").equals("Transport Research"));
		Assertions.assertTrue(map.containsKey("euromarine") && map.get("euromarine").equals("EuroMarine"));
		Assertions.assertTrue(map.containsKey("ifremer") && map.get("ifremer").equals("Ifremer"));
		Assertions.assertTrue(map.containsKey("oa-pg") && map.get("oa-pg").equals("EC Post-Grant Open Access Pilot"));
		Assertions
			.assertTrue(
				map.containsKey("science-innovation-policy")
					&& map.get("science-innovation-policy").equals("Science and Innovation Policy Studies"));
		Assertions.assertTrue(map.containsKey("covid-19") && map.get("covid-19").equals("COVID-19"));
		Assertions.assertTrue(map.containsKey("enermaps") && map.get("enermaps").equals("Energy Research"));
	}

}
