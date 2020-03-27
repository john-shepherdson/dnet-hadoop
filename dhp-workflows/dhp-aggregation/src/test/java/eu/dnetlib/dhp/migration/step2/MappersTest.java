package eu.dnetlib.dhp.migration.step2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;

@ExtendWith(MockitoExtension.class)
public class MappersTest {

	@Mock
	private Map<String, String> code2name;

	@BeforeEach
	void setUp() throws Exception {
		when(code2name.get(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
	}

	@Test
	void testPublication() throws IOException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("oaf_record.xml"));

		final List<Oaf> list = new OafToOafMapper(code2name).processMdRecord(xml);

		assertEquals(3, list.size());
		assertTrue(list.get(0) instanceof Publication);
		assertTrue(list.get(1) instanceof Relation);
		assertTrue(list.get(2) instanceof Relation);

		final Publication p = (Publication) list.get(0);
		final Relation r1 = (Relation) list.get(1);
		final Relation r2 = (Relation) list.get(2);

		assertValidId(p.getId());
		assertTrue(StringUtils.isNotBlank(p.getTitle().get(0).getValue()));
		assertTrue(p.getAuthor().size() > 0);
		assertTrue(p.getSubject().size() > 0);
		assertTrue(StringUtils.isNotBlank(p.getJournal().getIssnOnline()));
		assertTrue(StringUtils.isNotBlank(p.getJournal().getName()));

		assertValidId(r1.getSource());
		assertValidId(r2.getSource());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));
	}

	@Test
	void testDataset() throws IOException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("odf_dataset.xml"));

		final List<Oaf> list = new OdfToOafMapper(code2name).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Dataset);

		final Dataset d = (Dataset) list.get(0);

		assertValidId(d.getId());
		assertTrue(StringUtils.isNotBlank(d.getTitle().get(0).getValue()));
		assertTrue(d.getAuthor().size() > 0);
		assertTrue(d.getSubject().size() > 0);
	}

	@Test
	void testSoftware() throws IOException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("odf_software.xml"));

		final List<Oaf> list = new OdfToOafMapper(code2name).processMdRecord(xml);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Software);

		final Software s = (Software) list.get(0);

		assertValidId(s.getId());
		assertTrue(StringUtils.isNotBlank(s.getTitle().get(0).getValue()));
		assertTrue(s.getAuthor().size() > 0);
		assertTrue(s.getSubject().size() > 0);
	}

	private void assertValidId(final String id) {
		assertEquals(49, id.length());
		assertEquals('|', id.charAt(2));
		assertEquals(':', id.charAt(15));
		assertEquals(':', id.charAt(16));
	}
}
