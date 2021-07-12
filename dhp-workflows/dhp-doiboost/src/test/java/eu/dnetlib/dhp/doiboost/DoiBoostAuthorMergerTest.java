
package eu.dnetlib.dhp.doiboost;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.neethi.Assertion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.doiboost.DoiBoostAuthorMerger;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class DoiBoostAuthorMergerTest {

	private String publicationsBasePath;

	private List<List<Author>> authors;


	@BeforeEach
	public void setUp() throws Exception {

		publicationsBasePath = Paths
			.get(DoiBoostAuthorMergerTest.class.getResource("/eu/dnetlib/dhp/doiboost").toURI())
			.toFile()
			.getAbsolutePath();

	}

	@Test
	public void mergeTestOrcid() {

		authors = readSample(publicationsBasePath + "/matching_authors_first.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());

		for (List<Author> authors1 : authors) {
			System.out.println("List " + (authors.indexOf(authors1) + 1));
			for (Author author : authors1) {
				System.out.println(authorToString(author));
			}
		}

		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(10, merge.size());

		Assertions.assertEquals(3, merge.stream().filter(a -> a.getPid() != null).count());

		merge
			.stream()
			.filter(a -> a.getPid() != null)
			.forEach(
				a -> Assertions
					.assertTrue(
						a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))));
		merge.stream().filter(a -> a.getPid() != null).forEach(a -> {
			try {
				System.out.println(new ObjectMapper().writeValueAsString(a));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

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

	@Test
	public void mergeTestMAG() {

		authors = readSample(publicationsBasePath + "/matching_authors_second", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());

		for (List<Author> authors1 : authors) {
			System.out.println("List " + (authors.indexOf(authors1) + 1));
			for (Author author : authors1) {
				System.out.println(authorToString(author));
			}
		}

		List<Author> merge = DoiBoostAuthorMerger.merge(authors, true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(10, merge.size());

		Assertions.assertEquals(10, merge.stream().filter(a -> a.getPid() != null).count());

		merge
				.stream()
				.filter(a -> a.getPid() != null)
				.forEach(
						a -> Assertions
								.assertTrue(
										a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals("URL"))));
		merge.stream().filter(a -> a.getPid() != null).forEach(a -> {
			try {
				System.out.println(new ObjectMapper().writeValueAsString(a));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

	}


	@Test
	public void mergeTestCrossrefEmpty1() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/empty_crossref_authors_first.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(3, merge.size());

		Assertions.assertEquals(3, merge.stream().filter(a -> a.getPid() != null).count());

		merge
				.stream()
				.filter(a -> a.getPid() != null)
				.forEach(
						a -> Assertions
								.assertTrue(
										a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))));
		merge.stream().filter(a -> a.getPid() != null).forEach(a -> {
			try {
				System.out.println(new ObjectMapper().writeValueAsString(a));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

		System.out.println(new ObjectMapper().writeValueAsString(merge));

	}


	@Test
	public void mergeTestCrossrefEmpty2() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/empty_crossref_authors_second.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());



		List<Author> merge = DoiBoostAuthorMerger.merge(authors, false);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(10, merge.size());

		Assertions.assertEquals(10, merge.stream().filter(a -> a.getPid() != null).count());

		merge
				.stream()
				.filter(a -> a.getPid() != null)
				.forEach(
						a -> Assertions
								.assertTrue(
										a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals("URL"))));
		merge.stream().filter(a -> a.getPid() != null).forEach(a -> {
			try {
				System.out.println(new ObjectMapper().writeValueAsString(a));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

		Assertions.assertTrue(3 == merge.stream().filter(a -> a.getPid() !=null)
		.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());

	}

	@Test
	public void mergeTestCrossrefEmpty3() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/empty_crossref_author_third.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(10, merge.size());

		Assertions.assertEquals(10, merge.stream().filter(a -> a.getPid() != null).count());

		merge
				.stream()
				.filter(a -> a.getPid() != null)
				.forEach(
						a -> Assertions
								.assertTrue(
										a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals("URL"))));

		Assertions.assertTrue(3 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());


	}


	@Test
	public void mergeTestCrossrefEmpty4() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/empty_crossref_author_fourth.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

		Assertions.assertEquals(3, merge.size());

		Assertions.assertEquals(3, merge.stream().filter(a -> a.getPid() != null).count());


		Assertions.assertTrue(3 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());


	}

	@Test
	public void shouldMergeTest1() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/should_appear_author1.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		Assertions.assertTrue(6 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());

		Assertions.assertTrue(34 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING))).count());

		merge.stream().filter(a -> a.getRank() == 26)
				.forEach(a ->
						Assertions.assertTrue(a.getPid()
								.stream()
								.anyMatch(pid -> pid.getValue().equals("0000-0002-2445-5275")
										&& pid.getQualifier().getClassid().equals(ModelConstants.ORCID)
								)
						)
				);


	}

	@Test
	public void shouldMergeTest2() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/should_appear_author2.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);



		Assertions.assertTrue(5 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());

		Assertions.assertTrue(34 == merge.stream().filter(a -> a.getPid() !=null)
				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING))).count());

		merge.stream().filter(a -> a.getFullname().equals("da luz geraldo eduardo"))
				.forEach(a ->
						Assertions.assertTrue(a.getPid()
								.stream()
								.anyMatch(pid -> pid.getValue().equals("http://orcid.org/0000-0003-2434-0387")
										&& pid.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING)
								)
						)
				);


	}

	@Test
	public void shouldNotMergeTest1() throws JsonProcessingException {

		authors = readSample(publicationsBasePath + "/should_appear_author3.json", Publication.class)
				.stream()
				.map(p -> p._2().getAuthor())
				.collect(Collectors.toList());


		List<Author> merge = DoiBoostAuthorMerger.merge(authors,  true);

		System.out.println("Merge ");
		for (Author author : merge) {
			System.out.println(authorToString(author));
		}

//		Assertions.assertTrue(5 == merge.stream().filter(a -> a.getPid() !=null)
//				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID))).count());
//
//		Assertions.assertTrue(34 == merge.stream().filter(a -> a.getPid() !=null)
//				.filter(a -> a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING))).count());
//
//		merge.stream().filter(a -> a.getFullname().equals("da luz geraldo eduardo"))
//				.forEach(a ->
//						Assertions.assertTrue(a.getPid()
//								.stream()
//								.anyMatch(pid -> pid.getValue().equals("http://orcid.org/0000-0003-2434-0387")
//										&& pid.getQualifier().getClassid().equals(ModelConstants.ORCID_PENDING)
//								)
//						)
//				);


	}
}
