
package eu.dnetlib.dhp.collection.plugin.sftp;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

@Disabled
class SftpIteratorWithPasswordTest {

	private static final String baseUrl = "sftp://sftp.medra.org";
	private static final String username = "";
	private static final String password = "";
	private static final int port = 22;
	private static final boolean isRecursive = true;
	private static final Set<String> extensions = Sets.newHashSet("xml");

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	public void test() {
		final SftpIteratorWithPassword iterator = new SftpIteratorWithPassword(baseUrl, port, username, isRecursive,
			extensions, null, password);

		int count = 0;
		while (iterator.hasNext()) {
			final String s = iterator.next();
			System.out.println(s);
			count++;
		}
		System.out.println("TOTAL: " + count);
	}

	@Test
	public void testWithStartDate() {
		final String startDate = "2025-03-01";

		final SftpIteratorWithPassword iterator = new SftpIteratorWithPassword(baseUrl, port, username, isRecursive,
			extensions, startDate, password);

		int count = 0;
		while (iterator.hasNext()) {
			final String s = iterator.next();
			System.out.println(s);
			count++;
		}
		System.out.println("TOTAL: " + count);
	}
}
