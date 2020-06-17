
package eu.dnetlib.dhp.broker.oa.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TrustUtilsTest {

	private static final double THRESHOLD = 0.95;

	@Test
	public void rescaleTest_1() {
		verifyValue(-0.3, BrokerConstants.MIN_TRUST);
	}

	@Test
	public void rescaleTest_2() {
		verifyValue(0.0, BrokerConstants.MIN_TRUST);
	}

	@Test
	public void rescaleTest_3() {
		verifyValue(0.5, BrokerConstants.MIN_TRUST);
	}

	@Test
	public void rescaleTest_4() {
		verifyValue(0.95, BrokerConstants.MIN_TRUST);
	}

	@Test
	public void rescaleTest_5() {
		verifyValue(0.96, BrokerConstants.MIN_TRUST);
	}

	@Test
	public void rescaleTest_6() {
		verifyValue(0.97, 0.3f);
	}

	@Test
	public void rescaleTest_7() {
		verifyValue(0.98, 0.45f);
	}

	@Test
	public void rescaleTest_8() {
		verifyValue(0.99, 0.6f);
	}

	@Test
	public void rescaleTest_9() {
		verifyValue(1.00, BrokerConstants.MAX_TRUST);
	}

	@Test
	public void rescaleTest_10() {
		verifyValue(1.01, BrokerConstants.MAX_TRUST);
	}

	@Test
	public void rescaleTest_11() {
		verifyValue(2.00, BrokerConstants.MAX_TRUST);
	}

	private void verifyValue(final double originalScore, final float expectedTrust) {
		final float trust = TrustUtils.rescale(originalScore, THRESHOLD);
		System.out.println(trust);
		assertTrue(Math.abs(trust - expectedTrust) < 0.01);
	}

}
