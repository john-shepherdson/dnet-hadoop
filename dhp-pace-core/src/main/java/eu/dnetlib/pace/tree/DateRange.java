
package eu.dnetlib.pace.tree;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("dateRange")
public class DateRange extends AbstractStringComparator {

	int YEAR_RANGE;

	public DateRange(Map<String, String> params) {
		super(params, new com.wcohen.ss.JaroWinkler());
		YEAR_RANGE = Integer.parseInt(params.getOrDefault("year_range", "3"));
	}

	public DateRange(final double weight) {
		super(weight, new com.wcohen.ss.JaroWinkler());
	}

	protected DateRange(final double weight, final AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	public static boolean isNumeric(String str) {
		return str.matches("\\d+"); // match a number with optional '-' and decimal.
	}

	@Override
	public double distance(final String a, final String b, final Config conf) {
		if (a.isEmpty() || b.isEmpty()) {
			return -1.0; // return -1 if a field is missing
		}

		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
			LocalDate d1 = LocalDate.parse(a, formatter);
			LocalDate d2 = LocalDate.parse(b, formatter);
			Period period = Period.between(d1, d2);

			return period.getYears() <= YEAR_RANGE ? 1.0 : 0.0;
		} catch (DateTimeException e) {
			return -1.0;
		}

	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(final double d) {
		return d;
	}
}
