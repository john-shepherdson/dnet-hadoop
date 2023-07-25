
package eu.dnetlib.pace.tree;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("domainExactMatch")
public class DomainExactMatch extends ExactMatchIgnoreCase {

	public DomainExactMatch(final Map<String, String> params) {
		super(params);
	}

	@Override
	protected String toString(final Object f) {

		try {
			return asUrl(super.toString(f)).getHost();
		} catch (MalformedURLException e) {
			return "";
		}
	}

	private URL asUrl(final String value) throws MalformedURLException {
		return new URL(value);
	}
}
