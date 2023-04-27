
package eu.dnetlib.dhp.sx.graph.scholix;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.sx.scholix.Scholix;
import eu.dnetlib.dhp.schema.sx.scholix.ScholixFlat;

public class ScholixFlatTest {

	@Test
	public void flattenScholixTest() throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		InputStream gzipStream = new GZIPInputStream(getClass().getResourceAsStream("scholix_records.gz"));
		Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
		BufferedReader buffered = new BufferedReader(decoder);
		String line;
		FileWriter myWriter = new FileWriter("/Users/sandro/Downloads/records");
		while ((line = buffered.readLine()) != null) {
			final Scholix s = mapper.readValue(line, Scholix.class);
			final ScholixFlat flat = ScholixUtils.flattenizeScholix(s, line);
			assertNotNull(s);
			assertNotNull(flat);
			assertEquals(s.getIdentifier(), flat.getIdentifier());
			assertEquals(s.getRelationship().getName(), flat.getRelationType());
			assertEquals(s.getSource().getObjectType(), flat.getSourceType());
			assertEquals(s.getSource().getObjectSubType(), flat.getSourceSubType());
			myWriter.write(mapper.writeValueAsString(flat));
			myWriter.write("\n");

		}

		myWriter.close();

	}
}
