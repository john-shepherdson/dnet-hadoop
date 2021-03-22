
package eu.dnetlib.dhp.schema.oaf;

import static org.junit.jupiter.api.Assertions.*;

import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergeTest {

	OafEntity oaf;

	@BeforeEach
	public void setUp() {
		oaf = new Publication();
	}

	@Test
	public void mergeListsTest() {

		// string list merge test
		List<String> a = Arrays.asList("a", "b", "c", "e");
		List<String> b = Arrays.asList("a", "b", "c", "d");
		List<String> c = null;

		System.out.println("merge result 1 = " + oaf.mergeLists(a, b));

		System.out.println("merge result 2 = " + oaf.mergeLists(a, c));

		System.out.println("merge result 3 = " + oaf.mergeLists(c, c));
	}

	@Test
	public void mergePublicationCollectedFromTest() {

		Publication a = new Publication();
		Publication b = new Publication();

		a.setCollectedfrom(Arrays.asList(setKV("a", "open"), setKV("b", "closed")));
		b.setCollectedfrom(Arrays.asList(setKV("A", "open"), setKV("b", "Open")));

		a.mergeFrom(b);

		assertNotNull(a.getCollectedfrom());
		assertEquals(3, a.getCollectedfrom().size());
	}

	@Test
	public void mergePublicationSubjectTest() {

		Publication a = new Publication();
		Publication b = new Publication();

		a.setSubject(Arrays.asList(setSP("a", "open", "classe"), setSP("b", "open", "classe")));
		b.setSubject(Arrays.asList(setSP("A", "open", "classe"), setSP("c", "open", "classe")));

		a.mergeFrom(b);

		assertNotNull(a.getSubject());
		assertEquals(3, a.getSubject().size());
	}

	@Test
	public void mergeRelationTest() {

		Relation a = createRel(null, null);
		Relation b = createRel(null, null);
		a.mergeFrom(b);
		assertEquals(a, b);

		a = createRel(true, null);
		b = createRel(null, null);
		a.mergeFrom(b);
		assertEquals(true, a.getValidated());

		a = createRel(true, null);
		b = createRel(false, null);
		a.mergeFrom(b);
		assertEquals(true, a.getValidated());

		a = createRel(true, null);
		b = createRel(true, "2016-04-05T12:41:19.202Z");
		a.mergeFrom(b);
		assertEquals("2016-04-05T12:41:19.202Z", a.getValidationDate());

		a = createRel(true, "2016-05-07T12:41:19.202Z");
		b = createRel(true, "2016-04-05T12:41:19.202Z");
		a.mergeFrom(b);
		assertEquals("2016-04-05T12:41:19.202Z", a.getValidationDate());
	}

	@Test
	public void mergeRelationTestParseException() {
		assertThrows(DateTimeParseException.class, () -> {
			Relation a = createRel(true, "2016-04-05");
			Relation b = createRel(true, "2016-04-05");
			a.mergeFrom(b);
		});
	}

	private Relation createRel(Boolean validated, String validationDate) {
		Relation rel = new Relation();
		rel.setSource("1");
		rel.setTarget("2");
		rel.setRelType("reltype");
		rel.setSubRelType("subreltype");
		rel.setRelClass("relclass");
		rel.setValidated(validated);
		rel.setValidationDate(validationDate);
		return rel;
	}

	private KeyValue setKV(final String key, final String value) {

		KeyValue k = new KeyValue();

		k.setKey(key);
		k.setValue(value);

		return k;
	}

	private StructuredProperty setSP(
		final String value, final String schema, final String classname) {
		StructuredProperty s = new StructuredProperty();
		s.setValue(value);
		Qualifier q = new Qualifier();
		q.setClassname(classname);
		q.setClassid(classname);
		q.setSchemename(schema);
		q.setSchemeid(schema);
		s.setQualifier(q);
		return s;
	}
}
