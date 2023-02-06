
package eu.dnetlib.dhp.oa.graph.raw.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;

class VerifyNsPrefixPredicateTest {

	private VerifyNsPrefixPredicate predicate;

	@BeforeEach
	void setUp() throws Exception {
		predicate = new VerifyNsPrefixPredicate("corda,nsf,wt");
	}

	@Test
	void testTestValue() {
		assertFalse(predicate.testValue("corda__2020"));
		assertFalse(predicate.testValue("nsf________"));
		assertFalse(predicate.testValue("nsf"));
		assertFalse(predicate.testValue("corda"));
		assertFalse(predicate.testValue("10|corda_______::fjkdsfjksdhfksj"));
		assertFalse(predicate.testValue("20|corda_______::fjkdsfjksdhfksj"));

		assertTrue(predicate.testValue("xxxxxx_____"));
		assertTrue(predicate.testValue("10|xxxxxx_____::sdasdasaddasad"));

		assertTrue(predicate.testValue(null));
		assertTrue(predicate.testValue(""));
	}

	@Test
	void testTest_ds_true() {

		final Datasource ds = new Datasource();
		ds.setNamespaceprefix("xxxxxx______");

		assertTrue(predicate.test(ds));
	}

	@Test
	void testTest_ds_false() {
		final Datasource ds = new Datasource();
		ds.setNamespaceprefix("corda__2020");

		assertFalse(predicate.test(ds));
	}

	@Test
	void testTest_rel_true() {
		final Relation rel = new Relation();
		rel.setSource("10|yyyyyy______:sdfsfsffsdfs");
		rel.setTarget("10|xxxxxx______:sdfsfsffsdfs");
		assertTrue(predicate.test(rel));
	}

	@Test
	void testTest_rel_false() {
		final Relation rel = new Relation();
		rel.setSource("10|corda_______:sdfsfsffsdfs");
		rel.setTarget("10|xxxxxx______:sdfsfsffsdfs");
		assertFalse(predicate.test(rel));
	}

	@Test
	void testTest_proj_true() {
		final Project p = new Project();
		p.setId("10|xxxxxx______:sdfsfsffsdfs");
		assertTrue(predicate.test(p));
	}

	@Test
	void testTest_proj_false() {
		final Project p = new Project();
		p.setId("10|corda_____:sdfsfsffsdfs");
		assertFalse(predicate.test(p));
	}

}
