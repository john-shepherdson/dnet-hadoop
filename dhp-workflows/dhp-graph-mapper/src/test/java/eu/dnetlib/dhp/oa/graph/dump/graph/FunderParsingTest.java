
package eu.dnetlib.dhp.oa.graph.dump.graph;

import org.dom4j.DocumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.dump.oaf.graph.Funder;

public class FunderParsingTest {

	@Test
	public void testFunderTwoLevels() throws DocumentException {

		String funding_Stream = "<fundingtree><funder><id>nsf_________::NSF</id><shortname>NSF</shortname><name>National Science "
			+
			"Foundation</name><jurisdiction>US</jurisdiction></funder><funding_level_1><id>nsf_________::NSF::CISE/OAD::CISE/CCF</id><description>Division "
			+
			"of Computing and Communication Foundations</description><name>Division of Computing and Communication " +
			"Foundations</name><parent><funding_level_0><id>nsf_________::NSF::CISE/OAD</id><description>Directorate for "
			+
			"Computer &amp; Information Science &amp; Engineering</description><name>Directorate for Computer &amp; " +
			"Information Science &amp; Engineering</name><parent/><class>nsf:fundingStream</class></funding_level_0></parent></funding_level_1></fundingtree>";

		Funder f = DumpGraphEntities.getFunder(funding_Stream);

		Assertions.assertEquals("NSF", f.getShortName());
		Assertions.assertEquals("National Science Foundation", f.getName());
		Assertions.assertEquals("US", f.getJurisdiction());

		Assertions.assertEquals("NSF::CISE/OAD::CISE/CCF", f.getFunding_stream().getId());
		Assertions
			.assertEquals(
				"Directorate for Computer & Information Science & Engineering - Division of Computing and Communication Foundations",
				f.getFunding_stream().getDescription());

	}

	@Test
	public void testFunderThreeeLevels() throws DocumentException {
		String funding_stream = "<fundingtree><funder><id>ec__________::EC</id>" +
			"<shortname>EC</shortname>" +
			"<name>European Commission</name>" +
			"<jurisdiction>EU</jurisdiction>" +
			"</funder><funding_level_2>" +
			"<id>ec__________::EC::H2020::ERC::ERC-COG</id>" +
			"<description>Consolidator Grant</description>" +
			"<name>ERC-COG</name><class>ec:h2020toas</class>" +
			"<parent><funding_level_1><id>ec__________::EC::H2020::ERC</id>" +
			"<description>European Research Council</description>" +
			"<name>ERC</name><class>ec:h2020fundings</class><parent>" +
			"<funding_level_0><id>ec__________::EC::H2020</id><name>H2020</name>" +
			"<description>Horizon 2020 Framework Programme</description><parent/>" +
			"<class>ec:h2020fundings</class></funding_level_0></parent></funding_level_1></parent></funding_level_2></fundingtree>";

		Funder f = DumpGraphEntities.getFunder(funding_stream);

		Assertions.assertEquals("EC", f.getShortName());
		Assertions.assertEquals("European Commission", f.getName());
		Assertions.assertEquals("EU", f.getJurisdiction());

		Assertions.assertEquals("EC::H2020::ERC::ERC-COG", f.getFunding_stream().getId());
		Assertions
			.assertEquals(
				"Horizon 2020 Framework Programme - European Research Council - Consolidator Grant",
				f.getFunding_stream().getDescription());

	}
}
