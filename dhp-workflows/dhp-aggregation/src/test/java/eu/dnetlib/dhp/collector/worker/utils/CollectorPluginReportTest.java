
package eu.dnetlib.dhp.collector.worker.utils;

import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.worker.CollectorPluginReport;

public class CollectorPluginReportTest {

	@Test
	public void testSerialize() throws IOException {
		CollectorPluginReport r1 = new CollectorPluginReport();
		r1.put("a", "b");
		r1.setSuccess(true);

		String s = MAPPER.writeValueAsString(r1);

		Assertions.assertNotNull(s);

		CollectorPluginReport r2 = MAPPER.readValue(s, CollectorPluginReport.class);

		Assertions.assertTrue(r2.isSuccess(), "should be true");
	}

}
