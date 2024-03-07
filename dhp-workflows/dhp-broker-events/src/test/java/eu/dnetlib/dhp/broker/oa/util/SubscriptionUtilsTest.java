
package eu.dnetlib.dhp.broker.oa.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class SubscriptionUtilsTest {

	@Test
	void testVerifyListSimilar() {
		assertTrue(SubscriptionUtils.verifyListSimilar(Arrays.asList("Michele Artini", "Claudio Atzori"), "artini"));
		assertFalse(SubscriptionUtils.verifyListSimilar(Arrays.asList("Michele Artini", "Claudio Atzori"), "bardi"));
	}

	@Test
	void testVerifyListExact() {
		assertTrue(SubscriptionUtils.verifyListExact(Arrays.asList("Java", "Perl"), "perl"));
		assertFalse(SubscriptionUtils.verifyListExact(Arrays.asList("Java", "Perl"), "C"));
	}

	@Test
	void testVerifySimilar() {
		assertTrue(SubscriptionUtils.verifySimilar("Java Programming", "java"));
		assertFalse(SubscriptionUtils.verifySimilar("Java Programming", "soap"));
	}

	@Test
	void testVerifyFloatRange() {
		assertTrue(SubscriptionUtils.verifyFloatRange(0.5f, "0.4", "0.6"));
		assertFalse(SubscriptionUtils.verifyFloatRange(0.8f, "0.4", "0.6"));
		assertTrue(SubscriptionUtils.verifyFloatRange(0.5f, "", ""));
	}

	@Test
	void testVerifyDateRange() {
		final long date = 1282738478000l; // 25 August 2010

		assertTrue(SubscriptionUtils.verifyDateRange(date, "2010-01-01", "2011-01-01"));
		assertFalse(SubscriptionUtils.verifyDateRange(date, "2020-01-01", "2021-01-01"));

		assertTrue(SubscriptionUtils.verifyDateRange(date, "2010-01-01", "NULL"));
		assertTrue(SubscriptionUtils.verifyDateRange(date, "2010-01-01", null));
		assertTrue(SubscriptionUtils.verifyDateRange(date, "NULL", "2011-01-01"));
		assertTrue(SubscriptionUtils.verifyDateRange(date, null, "2011-01-01"));
		assertTrue(SubscriptionUtils.verifyDateRange(date, "NULL", "NULL"));
		assertTrue(SubscriptionUtils.verifyDateRange(date, null, null));

		assertFalse(SubscriptionUtils.verifyDateRange(date, "2020-01-01", null));
		assertFalse(SubscriptionUtils.verifyDateRange(date, "2020-01-01", "NULL"));
		assertFalse(SubscriptionUtils.verifyDateRange(date, null, "2005-01-01"));
		assertFalse(SubscriptionUtils.verifyDateRange(date, "NULL", "2005-01-01"));
	}

	@Test
	void testVerifyExact() {
		assertTrue(SubscriptionUtils.verifyExact("Java Programming", "java programming"));
		assertFalse(SubscriptionUtils.verifyExact("Java Programming", "soap programming"));
	}

}
