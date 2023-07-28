
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

class EntityMergerTest implements Serializable {

	private List<Tuple2<String, Publication>> publications;
	private List<Tuple2<String, Publication>> publications2;
	private List<Tuple2<String, Publication>> publications3;
	private List<Tuple2<String, Publication>> publications4;
	private List<Tuple2<String, Publication>> publications5;

	private String testEntityBasePath;
	private DataInfo dataInfo;
	private final String dedupId = "00|dedup_id::1";
	private Publication pub_top;

	@BeforeEach
	public void setUp() throws Exception {

		testEntityBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/json").toURI())
			.toFile()
			.getAbsolutePath();

		publications = readSample(testEntityBasePath + "/publication_merge.json", Publication.class);
		publications2 = readSample(testEntityBasePath + "/publication_merge2.json", Publication.class);
		publications3 = readSample(testEntityBasePath + "/publication_merge3.json", Publication.class);
		publications4 = readSample(testEntityBasePath + "/publication_merge4.json", Publication.class);
		publications5 = readSample(testEntityBasePath + "/publication_merge5.json", Publication.class);

		pub_top = getTopPub(publications);

		dataInfo = setDI();

	}

	@Test
	void softwareMergerTest() throws InstantiationException, IllegalAccessException, InvocationTargetException {

		List<Tuple2<String, Software>> softwares = readSample(
			testEntityBasePath + "/software_merge.json", Software.class);

		Software merged = DedupRecordFactory
			.entityMerger(dedupId, softwares.iterator(), 0, dataInfo, Software.class);

		assertEquals("OPEN SOURCE", merged.getBestaccessright().getClassid());

		assertEquals(dedupId, merged.getId());

	}

	@Test
	void publicationMergerTest() throws InstantiationException, IllegalAccessException, InvocationTargetException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications.iterator(), 0, dataInfo, Publication.class);

		// verify id
		assertEquals(dedupId, pub_merged.getId());

		assertEquals(pub_top.getJournal().getName(), pub_merged.getJournal().getName());
		assertEquals(pub_top.getJournal().getIssnOnline(), pub_merged.getJournal().getIssnOnline());
		assertEquals(pub_top.getJournal().getIssnLinking(), pub_merged.getJournal().getIssnLinking());
		assertEquals(pub_top.getJournal().getIssnPrinted(), pub_merged.getJournal().getIssnPrinted());
		assertEquals(pub_top.getJournal().getIss(), pub_merged.getJournal().getIss());
		assertEquals(pub_top.getJournal().getEp(), pub_merged.getJournal().getEp());
		assertEquals(pub_top.getJournal().getSp(), pub_merged.getJournal().getSp());
		assertEquals(pub_top.getJournal().getVol(), pub_merged.getJournal().getVol());
		assertEquals(pub_top.getJournal().getConferencedate(), pub_merged.getJournal().getConferencedate());
		assertEquals(pub_top.getJournal().getConferenceplace(), pub_merged.getJournal().getConferenceplace());
		assertEquals("OPEN", pub_merged.getBestaccessright().getClassid());
		assertEquals(pub_top.getResulttype(), pub_merged.getResulttype());
		assertEquals(pub_top.getLanguage(), pub_merged.getLanguage());
		assertEquals(pub_top.getPublisher(), pub_merged.getPublisher());
		assertEquals(pub_top.getEmbargoenddate(), pub_merged.getEmbargoenddate());
		assertEquals(pub_top.getResourcetype().getClassid(), "");
		assertEquals(pub_top.getDateoftransformation(), pub_merged.getDateoftransformation());
		assertEquals(pub_top.getOaiprovenance(), pub_merged.getOaiprovenance());
		assertEquals(pub_top.getDateofcollection(), pub_merged.getDateofcollection());
		assertEquals(3, pub_merged.getInstance().size());
		assertEquals(2, pub_merged.getCountry().size());
		assertEquals(0, pub_merged.getSubject().size());
		assertEquals(2, pub_merged.getTitle().size());
		assertEquals(0, pub_merged.getRelevantdate().size());
		assertEquals(0, pub_merged.getDescription().size());
		assertEquals(0, pub_merged.getSource().size());
		assertEquals(0, pub_merged.getFulltext().size());
		assertEquals(0, pub_merged.getFormat().size());
		assertEquals(0, pub_merged.getContributor().size());
		assertEquals(0, pub_merged.getCoverage().size());
		assertEquals(0, pub_merged.getContext().size());
		assertEquals(0, pub_merged.getExternalReference().size());
		assertEquals(3, pub_merged.getOriginalId().size());
		assertEquals(3, pub_merged.getCollectedfrom().size());
		assertEquals(1, pub_merged.getPid().size());
		assertEquals(0, pub_merged.getExtraInfo().size());

		// verify datainfo
		assertEquals(dataInfo, pub_merged.getDataInfo());

		// verify datepicker
		assertEquals("2018-09-30", pub_merged.getDateofacceptance().getValue());

		// verify authors
		assertEquals(13, pub_merged.getAuthor().size());
		assertEquals(4, AuthorMerger.countAuthorsPids(pub_merged.getAuthor()));

		// verify title
		int count = 0;
		for (StructuredProperty title : pub_merged.getTitle()) {
			if (title.getQualifier().getClassid().equals("main title"))
				count++;
		}
		assertEquals(1, count);
	}

	@Test
	void publicationMergerTest2() throws InstantiationException, IllegalAccessException, InvocationTargetException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications2.iterator(), 0, dataInfo, Publication.class);

		// verify id
		assertEquals(dedupId, pub_merged.getId());

		assertEquals(27, pub_merged.getAuthor().size());
	}

	@Test
	void publicationMergerTest3() throws InstantiationException, IllegalAccessException, InvocationTargetException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications3.iterator(), 0, dataInfo, Publication.class);

		// verify id
		assertEquals(dedupId, pub_merged.getId());
	}

	@Test
	void publicationMergerTest4()
		throws InstantiationException, IllegalStateException, IllegalAccessException, InvocationTargetException {

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications4.iterator(), 0, dataInfo, Publication.class);

		// verify id
		assertEquals(dedupId, pub_merged.getId());
	}

	@Test
	void publicationMergerTest5()
		throws InstantiationException, IllegalStateException, IllegalAccessException, InvocationTargetException {

		System.out
			.println(
				publications5
					.stream()
					.map(p -> p._2().getId())
					.collect(Collectors.toList()));

		Publication pub_merged = DedupRecordFactory
			.entityMerger(dedupId, publications5.iterator(), 0, dataInfo, Publication.class);

		// verify id
		assertEquals(dedupId, pub_merged.getId());
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
