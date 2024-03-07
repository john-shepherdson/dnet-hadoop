
package eu.dnetlib.dhp.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MdStoreClientTest {

	// @Test
	public void testMongoCollection() throws IOException {
		final MdstoreClient client = new MdstoreClient("mongodb://localhost:27017", "mdstore");

		final ObjectMapper mapper = new ObjectMapper();

		final List<MDStoreInfo> infos = client.mdStoreWithTimestamp("ODF", "store", "cleaned");

		infos.forEach(System.out::println);

		final String s = mapper.writeValueAsString(infos);

		Path fileName = Paths.get("/Users/sandro/mdstore_info.json");

		// Writing into the file
		Files.write(fileName, s.getBytes(StandardCharsets.UTF_8));

	}
}
