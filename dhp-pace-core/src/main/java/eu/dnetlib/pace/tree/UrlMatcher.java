
package eu.dnetlib.pace.tree;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("urlMatcher")
public class UrlMatcher extends Levenstein {

	private Map<String, String> params;

	public UrlMatcher(Map<String, String> params) {
		super(params);
		this.params = params;
	}

	public UrlMatcher(double weight, Map<String, String> params) {
		super(weight);
		this.params = params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	@Override
	public double distance(String a, String b, final Config conf) {
		final URL urlA = asUrl(a);
		final URL urlB = asUrl(b);

		if (!urlA.getHost().equalsIgnoreCase(urlB.getHost())) {
			return 0.0;
		}

		Double hostW = Double.parseDouble(params.getOrDefault("host", "0.5"));
		Double pathW = Double.parseDouble(params.getOrDefault("path", "0.5"));

		if (StringUtils.isBlank(urlA.getPath()) || StringUtils.isBlank(urlB.getPath())) {
			return hostW * 0.5;
		}

		return hostW + pathW * super.distance(urlA.getPath(), urlB.getPath(), conf);
	}

	private URL asUrl(final String value) {
		try {
			return new URL(value);
		} catch (MalformedURLException e) {
			// should not happen as checked by pace typing
			throw new IllegalStateException("invalid URL: " + value);
		}
	}

	protected String toString(final Object object) {
		return toFirstString(object);
	}
}
