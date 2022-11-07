
package eu.dnetlib.dhp.transformation.xslt.utils;

import com.google.common.base.Function;

public class Capitalize implements Function<String, String> {

	@Override
	public String apply(String s) {
		return org.apache.commons.lang3.text.WordUtils.capitalize(s.toLowerCase());
	}
}
