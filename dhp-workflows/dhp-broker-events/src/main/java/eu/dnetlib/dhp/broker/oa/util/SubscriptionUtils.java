
package eu.dnetlib.dhp.broker.oa.util;

import java.text.ParseException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;

public class SubscriptionUtils {

	private static final long ONE_DAY = 86_400_000;

	public static boolean verifyListSimilar(final List<String> list, final String value) {
		return list.stream().anyMatch(s -> verifySimilar(s, value));
	}

	public static boolean verifyListExact(final List<String> list, final String value) {
		return list.stream().anyMatch(s -> verifyExact(s, value));
	}

	public static boolean verifySimilar(final String s1, final String s2) {
		for (final String part : s2.split("\\W+")) {
			if (!StringUtils.containsIgnoreCase(s1, part)) {
				return false;
			}
		}
		return true;
	}

	public static boolean verifyFloatRange(final float trust, final String min, final String max) {
		return trust >= NumberUtils.toFloat(min, 0) && trust <= NumberUtils.toFloat(max, 1);
	}

	public static boolean verifyDateRange(final long date, final String min, final String max) {
		try {
			return date >= DateUtils.parseDate(min, "yyyy-MM-dd").getTime()
				&& date < DateUtils.parseDate(max, "yyyy-MM-dd").getTime() + ONE_DAY;
		} catch (final ParseException e) {
			return false;
		}
	}

	public static boolean verifyExact(final String s1, final String s2) {
		return StringUtils.equalsIgnoreCase(s1, s2);
	}

}
