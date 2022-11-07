
package eu.dnetlib.dhp.oa.merge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

class AuthorMergerTest {

	private String publicationsBasePath;

	private List<List<Author>> authors;

	@BeforeEach
	public void setUp() throws Exception {

		publicationsBasePath = Paths
			.get(AuthorMergerTest.class.getResource("/eu/dnetlib/dhp/oa/merge").toURI())
			.toFile()
			.getAbsolutePath();

		authors = readSample(publicationsBasePath + "/publications_with_authors.json", Publication.class)
			.stream()
			.map(p -> p._2().getAuthor())
			.collect(Collectors.toList());

	}

	@Test
	void mergeTest() { // used in the dedup: threshold set to 0.95

		for (List<Author> authors1 : authors) {
			System.out.println("List " + (authors.indexOf(authors1) + 1));
			for (Author author : authors1) {
				System.out.println(authorToString(author));
			}
		}

		List<Author> merge = AuthorMerger.merge(authors);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(7, merge.size());

	}

	public <T> List<Tuple2<String, T>> readSample(String path, Class<T> clazz) {
		List<Tuple2<String, T>> res = new ArrayList<>();
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(path));
			String line = reader.readLine();
			while (line != null) {
				res
					.add(
						new Tuple2<>(
							MapDocumentUtil.getJPathString("$.id", line),
							new ObjectMapper().readValue(line, clazz)));
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return res;
	}

	public String authorToString(Author a) {

		String print = "Fullname = ";
		print += a.getFullname() + " pid = [";
		if (a.getPid() != null)
			for (StructuredProperty sp : a.getPid()) {
				print += sp.toComparableString() + " ";
			}
		print += "]";
		return print;
	}
}
