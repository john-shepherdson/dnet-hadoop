package eu.dnetlib.dhp.collection.plugin.sftp;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

// @Disabled
class SftpIteratorWithPasswordTest {

	private static final String baseUrl = "sftp://sftp.medra.org/MSFTP0001/OPENAIRE/OUTPUT";
	private static final String username = "";
	private static final String password = "";
	private static final int port = 22;
	private static final boolean isRecursive = true;
	private static final Set<String> extensions = Sets.newHashSet("xml");

	@BeforeEach
	void setUp() throws Exception {}

	@Test
	@Disabled
	public void testALL() throws IOException {
		final SftpIteratorWithPassword iterator = new SftpIteratorWithPassword(baseUrl, port, username, isRecursive,
				extensions, null, password);

		int count = 0;
		while (iterator.hasNext()) {
			final String s = iterator.next();
			if (StringUtils.isBlank(s)) {
				fail();
			}
			// System.out.println(s);
			System.out.println(count++);
		}
		System.out.println("TOTAL: " + count);

	}

	@Test
	@Disabled
	public void testOne() throws IOException {
		final SftpIteratorWithPassword iterator = new SftpIteratorWithPassword(baseUrl, port, username, isRecursive,
				extensions, null, password);
		if (iterator.hasNext()) {
			final String s = iterator.next();
			System.out.println(s);
			return;
		}
		fail();

	}

	@Test
	@Disabled
	public void testWithStartDate() throws IOException {
		final String startDate = "2025-11-05";

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
