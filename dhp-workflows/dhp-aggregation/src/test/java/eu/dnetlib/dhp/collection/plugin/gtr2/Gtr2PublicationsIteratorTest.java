
package eu.dnetlib.dhp.collection.plugin.gtr2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Iterator;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.collection.HttpClientParams;

class Gtr2PublicationsIteratorTest {

	private static final String baseURL = "https://gtr.ukri.org/gtr/api";

	private static final HttpClientParams clientParams = new HttpClientParams();

	@Test
	@Disabled
	public void testOne() throws Exception {
		System.out.println("one publication");

		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, null, null, null, clientParams);

		if (iterator.hasNext()) {
			final String res = iterator.next();
			assertNotNull(res);
			System.out.println(res);
		}
	}

	@Test
	@Disabled
	public void testPaging() throws Exception {
		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, null, "2", "2", clientParams);

		while (iterator.hasNext()) {
			Thread.sleep(300);
			final String res = iterator.next();
			assertNotNull(res);
			System.out.println(res);
		}
	}

	@Test
	@Disabled
	public void testOnePage() throws Exception {
		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, null, "12", "12", clientParams);
		final int count = iterateAndCount(iterator);
		assertEquals(20, count);
	}

	@Test
	@Disabled
	public void testIncrementalHarvestingNoRecords() throws Exception {
		System.out.println("incremental Harvesting");
		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, "2050-12-12T", "11", "13",
			clientParams);
		final int count = iterateAndCount(iterator);
		assertEquals(0, count);
	}

	@Test
	@Disabled
	public void testIncrementalHarvesting() throws Exception {
		System.out.println("incremental Harvesting");
		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, "2016-11-30", "11", "11", clientParams);
		final int count = iterateAndCount(iterator);
		assertEquals(20, count);
	}

	@Test
	@Disabled
	public void testCompleteHarvesting() throws Exception {
		System.out.println("testing complete harvesting");
		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseURL, null, null, null, clientParams);
		// TryIndentXmlString indenter = new TryIndentXmlString();
		// it.setEndAtPage(3);

		while (iterator.hasNext()) {
			final String res = iterator.next();
			assertNotNull(res);
			// System.out.println(res);
			// Scanner keyboard = new Scanner(System.in);
			// System.out.println("press enter for next record");
			// keyboard.nextLine();

		}
	}

	private int iterateAndCount(final Iterator<String> iterator) throws Exception {
		int i = 0;
		while (iterator.hasNext()) {
			assertNotNull(iterator.next());
			i++;
		}
		System.out.println("Got " + i + " publications");
		return i;
	}

}
