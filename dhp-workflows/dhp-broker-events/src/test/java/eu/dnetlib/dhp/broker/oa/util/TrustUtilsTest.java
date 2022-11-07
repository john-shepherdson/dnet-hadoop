
package eu.dnetlib.dhp.broker.oa.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import eu.dnetlib.broker.objects.OaBrokerAuthor;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;

public class TrustUtilsTest {

	private static final double THRESHOLD = 0.95;

	@Test
	void rescaleTest_1() {
		verifyValue(-0.3, BrokerConstants.MIN_TRUST);
	}

	@Test
	void rescaleTest_2() {
		verifyValue(0.0, BrokerConstants.MIN_TRUST);
	}

	@Test
	void rescaleTest_3() {
		verifyValue(0.5, BrokerConstants.MIN_TRUST);
	}

	@Test
	void rescaleTest_4() {
		verifyValue(0.95, BrokerConstants.MIN_TRUST);
	}

	@Test
	void rescaleTest_5() {
		verifyValue(0.96, BrokerConstants.MIN_TRUST);
	}

	@Test
	void rescaleTest_6() {
		verifyValue(0.97, 0.3f);
	}

	@Test
	void rescaleTest_7() {
		verifyValue(0.98, 0.45f);
	}

	@Test
	void rescaleTest_8() {
		verifyValue(0.99, 0.6f);
	}

	@Test
	void rescaleTest_9() {
		verifyValue(1.00, BrokerConstants.MAX_TRUST);
	}

	@Test
	void rescaleTest_10() {
		verifyValue(1.01, BrokerConstants.MAX_TRUST);
	}

	@Test
	void rescaleTest_11() {
		verifyValue(2.00, BrokerConstants.MAX_TRUST);
	}

	@Test
	void test() {
		final OaBrokerMainEntity r1 = new OaBrokerMainEntity();
		r1.getTitles().add("D-NET Service Package: Data Import");
		r1.getPids().add(new OaBrokerTypedValue("doi", "123"));
		r1.getCreators().add(new OaBrokerAuthor("Michele Artini", null));
		r1.getCreators().add(new OaBrokerAuthor("Claudio Atzori", null));

		final OaBrokerMainEntity r2 = new OaBrokerMainEntity();
		r2.getTitles().add("D-NET Service Package: Data Import");
		// r2.getPids().add(new OaBrokerTypedValue("doi", "123"));
		r2.getCreators().add(new OaBrokerAuthor("Michele Artini", null));
		// r2.getCreators().add(new OaBrokerAuthor("Claudio Atzori", null));

		System.out.println("TRUST: " + TrustUtils.calculateTrust(r1, r2));
	}

	private void verifyValue(final double originalScore, final float expectedTrust) {
		final float trust = TrustUtils.rescale(originalScore, THRESHOLD);
		System.out.println(trust);
		assertTrue(Math.abs(trust - expectedTrust) < 0.01);
	}

}
