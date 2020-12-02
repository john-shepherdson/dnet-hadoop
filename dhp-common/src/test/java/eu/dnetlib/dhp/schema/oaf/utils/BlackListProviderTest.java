
package eu.dnetlib.dhp.schema.oaf.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BlackListProviderTest {

	@Test
	public void blackListTest() {

		Assertions.assertNotNull(PidBlacklistProvider.getBlacklist());
		Assertions.assertNotNull(PidBlacklistProvider.getBlacklist().get("doi"));
		Assertions.assertTrue(PidBlacklistProvider.getBlacklist().get("doi").size() > 0);
		Assertions.assertNull(PidBlacklistProvider.getBlacklist("xxx"));
	}
}
