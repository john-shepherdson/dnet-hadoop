
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlackListProviderTest {

	@Test
	void blackListTest() {

		Assertions.assertNotNull(PidBlacklistProvider.getBlacklist());
		Assertions.assertNotNull(PidBlacklistProvider.getBlacklist().get("doi"));
		Assertions.assertTrue(PidBlacklistProvider.getBlacklist().get("doi").size() > 0);
		final Set<String> xxx = PidBlacklistProvider.getBlacklist("xxx");
		Assertions.assertNotNull(xxx);
		Assertions.assertEquals(0, xxx.size());
	}
}
