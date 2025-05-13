
package eu.dnetlib.dhp.collection.plugin.sftp;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

@Disabled
class SftpIteratorWithPasswordTest {

	private static final String baseUrl = "sftp://.../XXXX";
	private static final String username = "openaire";
	private static final String password = "XXXXX";
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

		while (iterator.hasNext()) {
			final String remotePath = iterator.next();
			System.out.println(remotePath);
		}
	}

}
