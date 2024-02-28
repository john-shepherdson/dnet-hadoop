
package eu.dnetlib.dhp.collection.plugin.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonUtils {
	public static final String XML_WRAP_TAG = "recordWrap";
	private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private static final String INVALID_XMLTAG_CHARS = "!\"#$%&'()*+,/;<=>?@[\\]^`{|}~,";

	private static final Log log = LogFactory.getLog(JsonUtils.class);

	/**
	 * cleanup in JSON-KeyName
	 * check W3C XML syntax: https://www.w3.org/TR/2006/REC-xml11-20060816/#sec-starttags for valid tag names
	 * and work-around for the JSON to XML converting of org.json.XML-package.
	 *
	 * @param input
	 * @return converted json object
	 */
	public static JSONObject cleanJsonObject(final JSONObject input) {
		if (null == input) {
			return null;
		}

		JSONObject result = new JSONObject();

		for (String key : input.keySet()) {
			Object value = input.opt(key);
			if (value != null) {
				result.put(cleanKey(key), cleanValue(value));
			}
		}

		return result;
	}

	private static Object cleanValue(Object object) {
		if (object instanceof JSONObject) {
			return cleanJsonObject((JSONObject) object);
		} else if (object instanceof JSONArray) {
			JSONArray array = (JSONArray) object;
			JSONArray res = new JSONArray();

			for (int i = array.length() - 1; i >= 0; i--) {
				res.put(i, cleanValue(array.opt(i)));
			}
			return res;
		} else if (object instanceof String) {
			String value = (String) object;

			// XML 1.0 Allowed characters
			// Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]

			return value
				.codePoints()
				.filter(
					cp -> cp == 0x9 || cp == 0xA || cp == 0xD || (cp >= 0x20 && cp <= 0xD7FF)
						|| (cp >= 0xE000 && cp <= 0xFFFD)
						|| (cp >= 0x10000 && cp <= 0x10FFFF))
				.collect(
					StringBuilder::new,
					StringBuilder::appendCodePoint,
					StringBuilder::append)
				.toString();
		}

		return object;
	}

	private static String cleanKey(String key) {
		if (key == null || key.isEmpty()) {
			return key;
		}

		// xml tag cannot begin with "-", ".", or a numeric digit.
		switch (key.charAt(0)) {
			case '-':
			case '.':
				key = "_" + key.substring(1);
				break;
		}

		if (Character.isDigit(key.charAt(0))) {
			if (key.matches("^[0-9]+$")) {
				// add prefix of only numbers in JSON Keynames with 'm_'
				key = "m_" + key;
			} else {
				// add prefix of startNumbers in JSON Keynames with 'n_'
				key = "n_" + key;
			}
		}

		StringBuilder res = new StringBuilder(key.length());
		for (int i = 0; i < key.length(); i++) {
			char c = key.charAt(i);

			// sequence of whitespaces are rendered as a single '_'
			if (Character.isWhitespace(c)) {
				while (i + 1 < key.length() && Character.isWhitespace(key.charAt(i + 1))) {
					i++;
				}
				res.append('_');
			}
			// remove invalid chars for xml tags with the expception of '=' and '/'
			else if (INVALID_XMLTAG_CHARS.indexOf(c) >= 0) {
				switch (c) {
					case '=':
						res.append('-');
						break;
					case '/':
						res.append('_');
						break;
					default:
						break;
				}
				// nothing
			}
			// all other chars are kept
			else {
				res.append(c);
			}
		}

		return res.toString();
	}

	static public String convertToXML(final String jsonRecord) {
		if (log.isTraceEnabled()) {
			log.trace("input json: " + jsonRecord);
		}

		JSONObject jsonObject = cleanJsonObject(new org.json.JSONObject(jsonRecord));
		String res = XML_HEADER + org.json.XML.toString(jsonObject, XML_WRAP_TAG); // wrap xml in single root element

		if (log.isTraceEnabled()) {
			log.trace("outout xml: " + res);
		}
		return res;
	}
}
