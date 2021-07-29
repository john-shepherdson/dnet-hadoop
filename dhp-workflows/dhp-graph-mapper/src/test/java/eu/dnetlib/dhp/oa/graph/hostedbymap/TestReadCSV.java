
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;

import eu.dnetlib.dhp.oa.graph.hostedbymap.model.UnibiGoldModel;

public class TestReadCSV {

	@Test
	public void testCSVUnibi() throws FileNotFoundException {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/hostedbymap/unibiGold.csv")
			.getPath();

		List<UnibiGoldModel> beans = new CsvToBeanBuilder(new FileReader(sourcePath))
			.withType(UnibiGoldModel.class)
			.build()
			.parse();

		ObjectMapper mapper = new ObjectMapper();

		beans.forEach(r -> {
			try {
				System.out.println(mapper.writeValueAsString(r));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

	}

	@Test
	public void testCSVUrlUnibi() throws IOException {

		URL csv = new URL("https://pub.uni-bielefeld.de/download/2944717/2944718/issn_gold_oa_version_4.csv");

		BufferedReader in = new BufferedReader(new InputStreamReader(csv.openStream()));
		ObjectMapper mapper = new ObjectMapper();

		new CsvToBeanBuilder(in)
			.withType(eu.dnetlib.dhp.oa.graph.hostedbymap.model.UnibiGoldModel.class)
			.build()
			.parse()
			.forEach(line ->

			{
				try {
					System.out.println(mapper.writeValueAsString(line));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			}

			);
	}

	@Test
	public void testCSVUrlDOAJ() throws IOException {

		URLConnection connection = new URL("https://doaj.org/csv").openConnection();
		connection
			.setRequestProperty(
				"User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
		connection.connect();

		BufferedReader in = new BufferedReader(
			new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
		// BufferedReader in = new BufferedReader(new FileReader("/tmp/DOAJ.csv"));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/DOAJ_1.csv")));
		String line = null;
		while ((line = in.readLine()) != null) {
			writer.println(line.replace("\\\"", "\""));
		}
		writer.close();
		in.close();
		in = new BufferedReader(new FileReader("/tmp/DOAJ_1.csv"));
		ObjectMapper mapper = new ObjectMapper();

		new CsvToBeanBuilder(in)
			.withType(eu.dnetlib.dhp.oa.graph.hostedbymap.model.DOAJModel.class)
			.withMultilineLimit(1)
			.build()
			.parse()
			.forEach(lline ->

			{
				try {
					System.out.println(mapper.writeValueAsString(lline));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			}

			);
	}
}
