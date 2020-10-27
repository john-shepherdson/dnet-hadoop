
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class EntityMergerTest implements Serializable {

	List<Tuple2<String, Publication>> publications;
	List<Tuple2<String, Publication>> publications2;

	String testEntityBasePath;
	DataInfo dataInfo;
	String dedupId = "dedup_id";
	Publication pub_top;

	@BeforeEach
	public void setUp() throws Exception {

		testEntityBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/json").toURI())
			.toFile()
			.getAbsolutePath();

		publications = readSample(testEntityBasePath + "/publication_merge.json", Publication.class);
		publications2 = readSample(testEntityBasePath + "/publication_merge2.json", Publication.class);

		pub_top = getTopPub(publications);

		dataInfo = setDI();

	}

	@Test
	public void softwareMergerTest() throws InstantiationException, IllegalAccessException {
		List<Tuple2<String, Software>> softwares = readSample(
			testEntityBasePath + "/software_merge.json", Software.class);

		Software merged = DedupRecordFactory
			.entityMerger(dedupId, softwares.iterator(), 0, dataInfo, Software.class);

		assertEquals(merged.getBestaccessright().getClassid(), "OPEN SOURCE");
	}

	@Test
	public void publicationMergerTest() throws InstantiationException, IllegalAccessException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications.iterator(), 0, dataInfo, Publication.class);

		assertEquals(dedupId, pub_merged.getId());

		assertEquals(pub_merged.getJournal(), pub_top.getJournal());
		assertEquals(pub_merged.getBestaccessright().getClassid(), "OPEN");
		assertEquals(pub_merged.getResulttype(), pub_top.getResulttype());
		assertEquals(pub_merged.getLanguage(), pub_merged.getLanguage());
		assertEquals(pub_merged.getPublisher(), pub_top.getPublisher());
		assertEquals(pub_merged.getEmbargoenddate(), pub_top.getEmbargoenddate());
		assertEquals(pub_merged.getResourcetype().getClassid(), "0004");
		assertEquals(pub_merged.getDateoftransformation(), pub_top.getDateoftransformation());
		assertEquals(pub_merged.getOaiprovenance(), pub_top.getOaiprovenance());
		assertEquals(pub_merged.getDateofcollection(), pub_top.getDateofcollection());
		assertEquals(pub_merged.getInstance().size(), 3);
		assertEquals(pub_merged.getCountry().size(), 2);
		assertEquals(pub_merged.getSubject().size(), 0);
		assertEquals(pub_merged.getTitle().size(), 2);
		assertEquals(pub_merged.getRelevantdate().size(), 0);
		assertEquals(pub_merged.getDescription().size(), 0);
		assertEquals(pub_merged.getSource().size(), 0);
		assertEquals(pub_merged.getFulltext().size(), 0);
		assertEquals(pub_merged.getFormat().size(), 0);
		assertEquals(pub_merged.getContributor().size(), 0);
		assertEquals(pub_merged.getCoverage().size(), 0);
		assertEquals(pub_merged.getContext().size(), 0);
		assertEquals(pub_merged.getExternalReference().size(), 0);
		assertEquals(pub_merged.getOriginalId().size(), 3);
		assertEquals(pub_merged.getCollectedfrom().size(), 3);
		assertEquals(pub_merged.getPid().size(), 1);
		assertEquals(pub_merged.getExtraInfo().size(), 0);

		// verify datainfo
		assertEquals(pub_merged.getDataInfo(), dataInfo);

		// verify datepicker
		assertEquals(pub_merged.getDateofacceptance().getValue(), "2018-09-30");

		// verify authors
		assertEquals(pub_merged.getAuthor().size(), 9);
		assertEquals(AuthorMerger.countAuthorsPids(pub_merged.getAuthor()), 4);

		// verify title
		int count = 0;
		for (StructuredProperty title : pub_merged.getTitle()) {
			if (title.getQualifier().getClassid().equals("main title"))
				count++;
		}
		assertEquals(count, 1);
	}

	@Test
	public void publicationMergerTest2() throws InstantiationException, IllegalAccessException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications2.iterator(), 0, dataInfo, Publication.class);

		assertEquals(pub_merged.getAuthor().size(), 27);
		// insert assertions here

	}

	public DataInfo setDI() {
		DataInfo dataInfo = new DataInfo();
		dataInfo.setTrust("0.9");
		dataInfo.setDeletedbyinference(false);
		dataInfo.setInferenceprovenance("testing");
		dataInfo.setInferred(true);
		return dataInfo;
	}

	public Publication getTopPub(List<Tuple2<String, Publication>> publications) {

		Double maxTrust = 0.0;
		Publication maxPub = new Publication();
		for (Tuple2<String, Publication> publication : publications) {
			Double pubTrust = Double.parseDouble(publication._2().getDataInfo().getTrust());
			if (pubTrust > maxTrust) {
				maxTrust = pubTrust;
				maxPub = publication._2();
			}
		}
		return maxPub;
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

}
