
package eu.dnetlib.dhp.monitoring;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class MongoLogsDumperTests {

	@Test
	public void logsDump() throws IOException {
		final MongoLogsDumper dumper = new MongoLogsDumper("localhost", 27017, "dnet_logs", "wf_logs", null);
		Iterator<AggregationLog> result = dumper.dumpLogs(1742826036670L);

		Path outputPath = Paths.get( "/tmp/aggregation_logs.txt");
		Files.createDirectories(outputPath.getParent());
		ObjectMapper mapper = new ObjectMapper();
		try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
			while (result.hasNext()) {
				AggregationLog log = result.next();
				writer.write(mapper.writeValueAsString(log));
				writer.newLine();
			}
		}

		System.out.println("Logs written to: " + outputPath.toAbsolutePath());



	}

}
