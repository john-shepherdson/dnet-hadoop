
package eu.dnetlib.dhp.collection.plugin.omicsdi;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

@Disabled
class OmicsDIDatanaseIteratorTest {

	private OmicsDIDatabaseIterator iter;

	private static final String BASE_URL = "https://www.omicsdi.org/ws";

	private static final String DB_REPO_NAME = "EGA";

	@BeforeEach
	void setUp() throws Exception {
		this.iter = new OmicsDIDatabaseIterator(BASE_URL, DB_REPO_NAME, 1000, new HttpClientParams());
	}

	@Test
	void testComplete() throws CollectorException {
		long count = 0;

		while (this.iter.hasNext()) {
			this.iter.next();
			count++;
		}

		System.out.println("TOTAL: " + count);
		assertTrue(count > 0);
	}

}
