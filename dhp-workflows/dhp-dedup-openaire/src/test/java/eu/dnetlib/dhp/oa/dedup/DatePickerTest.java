
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import com.clearspring.analytics.util.Lists;

public class DatePickerTest {

	Collection<String> dates = Lists.newArrayList();

	@Test
	public void testPickISO() {
		dates.add("2016-01-01T12:00:00Z");
		dates.add("2016-06-16T12:00:00Z");
		dates.add("2020-01-01T12:00:00Z");
		dates.add("2020-10-01T12:00:00Z");
		assertEquals("2020-10-01", DatePicker.pick(dates).getValue());
	}

	@Test
	public void testPickSimple() {
		dates.add("2016-01-01");
		dates.add("2016-06-16");
		dates.add("2020-01-01");
		dates.add("2020-10-01");
		assertEquals("2020-10-01", DatePicker.pick(dates).getValue());
	}

	@Test
	public void testPickFrequent() {
		dates.add("2016-02-01");
		dates.add("2016-02-01");
		dates.add("2016-02-01");
		dates.add("2020-10-01");
		assertEquals("2016-02-01", DatePicker.pick(dates).getValue());
	}

}
