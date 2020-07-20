
package eu.dnetlib.dhp.oa.graph.raw;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

@ExtendWith(MockitoExtension.class)
public class MappersTest {

	@Mock
	private VocabularyGroup vocs;

	@BeforeEach
	public void setUp() throws Exception {
		when(vocs.getTermAsQualifier(anyString(), anyString()))
			.thenAnswer(
				invocation -> OafMapperUtils
					.qualifier(
						invocation.getArgument(1), invocation.getArgument(1), invocation.getArgument(0),
						invocation.getArgument(0)));

		when(vocs.termExists(anyString(), anyString())).thenReturn(true);

	}

	@Test
	void testPublication() throws IOException {

		final String xml = IOUtils.toString(getClass().getResourceAsStream("oaf_record.xml"));

		final List<Oaf> list = new OafToOafMapper(vocs, false).processMdRecord(xml);

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Publication);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Publication p = (Publication) list.get(0);
		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertValidId(p.getId());

		assertTrue(p.getOriginalId().size() == 1);
		assertEquals("10.3897/oneeco.2.e13718", p.getOriginalId().get(0));

		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertFalse(p.getDataInfo().getInvisible());
		assertTrue(p.getSource().size() == 1);

		assertTrue(p.getAuthor().size() > 0);
		final Optional<Author> author = p
			.getAuthor()
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.findFirst();
		assertTrue(author.isPresent());

		final StructuredProperty pid = author
			.get()
			.getPid()
			.stream()
			.findFirst()
			.get();
		assertEquals("0000-0001-6651-1178", pid.getValue());
		assertEquals("ORCID", pid.getQualifier().getClassid());
		assertEquals("Open Researcher and Contributor ID", pid.getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemename());
		assertEquals("Votsi,Nefta", author.get().getFullname());
		assertEquals("Votsi", author.get().getSurname());
		assertEquals("Nefta", author.get().getName());

		assertTrue(p.getSubject().size() > 0);
		assertTrue(StringUtils.isNotBlank(p.getJournal().getIssnOnline()));
		assertTrue(StringUtils.isNotBlank(p.getJournal().getName()));

		assertTrue(p.getPid().size() > 0);
		assertEquals(p.getPid().get(0).getValue(), "10.3897/oneeco.2.e13718");
		assertEquals(p.getPid().get(0).getQualifier().getClassid(), "doi");

		assertNotNull(p.getInstance());
		assertTrue(p.getInstance().size() > 0);
		p
			.getInstance()
			.stream()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("0001", p.getInstance().get(0).getRefereed().getClassid());

		assertNotNull(p.getBestaccessright());
		assertEquals("OPEN", p.getBestaccessright().getClassid());
		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertValidId(r1.getCollectedfrom().get(0).getKey());
		assertValidId(r2.getCollectedfrom().get(0).getKey());
		assertNotNull(r1.getDataInfo());
		assertNotNull(r2.getDataInfo());
		assertNotNull(r1.getDataInfo().getTrust());
		assertNotNull(r2.getDataInfo().getTrust());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));

		// System.out.println(new ObjectMapper().writeValueAsString(p));
		// System.out.println(new ObjectMapper().writeValueAsString(r1));
		// System.out.println(new ObjectMapper().writeValueAsString(r2));
	}

	@Test
	void testPublicationInvisible() throws IOException {

		final String xml = IOUtils.toString(getClass().getResourceAsStream("oaf_record.xml"));

		final List<Oaf> list = new OafToOafMapper(vocs, true).processMdRecord(xml);

		assertTrue(list.size() > 0);
		assertTrue(list.get(0) instanceof Publication);

		final Publication p = (Publication) list.get(0);

		assertTrue(p.getDataInfo().getInvisible());

	}

	@Test
	void testDataset() throws IOException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("odf_dataset.xml"));

		final List<Oaf> list = new OdfToOafMapper(vocs, false).processMdRecord(xml);

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Dataset);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Dataset d = (Dataset) list.get(0);
		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertValidId(d.getId());
		assertTrue(d.getOriginalId().size() == 1);
		assertEquals("oai:zenodo.org:3234526", d.getOriginalId().get(0));
		assertValidId(d.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertTrue(d.getAuthor().size() > 0);

		final Optional<Author> author = d
			.getAuthor()
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.findFirst();
		assertTrue(author.isPresent());
		final StructuredProperty pid = author
			.get()
			.getPid()
			.stream()
			.findFirst()
			.get();
		assertEquals("0000-0001-9074-1619", pid.getValue());
		assertEquals("ORCID", pid.getQualifier().getClassid());
		assertEquals("Open Researcher and Contributor ID", pid.getQualifier().getClassname());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemeid());
		assertEquals(ModelConstants.DNET_PID_TYPES, pid.getQualifier().getSchemename());
		assertEquals("Baracchini, Theo", author.get().getFullname());
		assertEquals("Baracchini", author.get().getSurname());
		assertEquals("Theo", author.get().getName());

		assertEquals(1, author.get().getAffiliation().size());
		final Optional<Field<String>> opAff = author
			.get()
			.getAffiliation()
			.stream()
			.findFirst();
		assertTrue(opAff.isPresent());
		final Field<String> affiliation = opAff.get();
		assertEquals("ISTI-CNR", affiliation.getValue());

		assertTrue(d.getSubject().size() > 0);
		assertTrue(d.getInstance().size() > 0);
		assertTrue(d.getContext().size() > 0);
		assertTrue(d.getContext().get(0).getId().length() > 0);

		assertNotNull(d.getInstance());
		assertTrue(d.getInstance().size() > 0);
		d
			.getInstance()
			.stream()
			.forEach(i -> {
				assertNotNull(i.getAccessright());
				assertEquals("OPEN", i.getAccessright().getClassid());
			});
		assertEquals("0001", d.getInstance().get(0).getRefereed().getClassid());

		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertNotNull(r1.getDataInfo());
		assertNotNull(r2.getDataInfo());
		assertNotNull(r1.getDataInfo().getTrust());
		assertNotNull(r2.getDataInfo().getTrust());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));
	}

	@Test
	void testSoftware() throws IOException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("odf_software.xml"));

		final List<Oaf> list = new OdfToOafMapper(vocs, false).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Software);

		final Software s = (Software) list.get(0);

		assertValidId(s.getId());
		assertValidId(s.getCollectedfrom().get(0).getKey());
		assertTrue(StringUtils.isNotBlank(s.getTitle().get(0).getValue()));
		assertTrue(s.getAuthor().size() > 0);
		assertTrue(s.getSubject().size() > 0);
		assertTrue(s.getInstance().size() > 0);
	}

	private void assertValidId(final String id) {
		assertEquals(49, id.length());
		assertEquals('|', id.charAt(2));
		assertEquals(':', id.charAt(15));
		assertEquals(':', id.charAt(16));
	}
}
