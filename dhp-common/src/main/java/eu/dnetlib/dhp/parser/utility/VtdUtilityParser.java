
package eu.dnetlib.dhp.parser.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDNav;

/** Created by sandro on 9/29/16. */
public class VtdUtilityParser {

	public static List<Node> getTextValuesWithAttributes(
		final AutoPilot ap, final VTDNav vn, final String xpath, final List<String> attributes)
		throws VtdException {
		final List<Node> results = new ArrayList<>();
		try {
			ap.selectXPath(xpath);

			while (ap.evalXPath() != -1) {
				final Node currentNode = new Node();
				int t = vn.getText();
				if (t >= 0) {
					currentNode.setTextValue(vn.toNormalizedString(t));
				}
				currentNode.setAttributes(getAttributes(vn, attributes));
				results.add(currentNode);
			}
			return results;
		} catch (Exception e) {
			throw new VtdException(e);
		}
	}

	private static Map<String, String> getAttributes(final VTDNav vn, final List<String> attributes) {
		final Map<String, String> currentAttributes = new HashMap<>();
		if (attributes != null) {

			attributes
				.forEach(
					attributeKey -> {
						try {
							int attr = vn.getAttrVal(attributeKey);
							if (attr > -1) {
								currentAttributes.put(attributeKey, vn.toNormalizedString(attr));
							}
						} catch (Throwable e) {
							throw new RuntimeException(e);
						}
					});
		}
		return currentAttributes;
	}

	public static List<String> getTextValue(final AutoPilot ap, final VTDNav vn, final String xpath)
		throws VtdException {
		List<String> results = new ArrayList<>();
		try {
			ap.selectXPath(xpath);
			while (ap.evalXPath() != -1) {
				int t = vn.getText();
				if (t > -1)
					results.add(vn.toNormalizedString(t));
			}
			return results;
		} catch (Exception e) {
			throw new VtdException(e);
		}
	}

	public static String getSingleValue(final AutoPilot ap, final VTDNav nav, final String xpath)
		throws VtdException {
		try {
			ap.selectXPath(xpath);
			while (ap.evalXPath() != -1) {
				int it = nav.getText();
				if (it > -1)
					return nav.toNormalizedString(it);
			}
			return null;
		} catch (Exception e) {
			throw new VtdException(e);
		}
	}

	public static class Node {

		private String textValue;

		private Map<String, String> attributes;

		public String getTextValue() {
			return textValue;
		}

		public void setTextValue(final String textValue) {
			this.textValue = textValue;
		}

		public Map<String, String> getAttributes() {
			return attributes;
		}

		public void setAttributes(final Map<String, String> attributes) {
			this.attributes = attributes;
		}
	}
}
