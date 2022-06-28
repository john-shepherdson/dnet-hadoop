
package eu.dnetlib.dhp.collection.plugin.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JsonUtils {

	private static final Log log = LogFactory.getLog(JsonUtils.class);

	public static final String wrapName = "recordWrap";

	/**
	 * convert in JSON-KeyName 'whitespace(s)' to '_' and '/' to '_', '(' and ')' to ''
	 * check W3C XML syntax: https://www.w3.org/TR/2006/REC-xml11-20060816/#sec-starttags for valid tag names
	 * and work-around for the JSON to XML converting of org.json.XML-package.
	 *
	 * known bugs:     doesn't prevent     "key name":" ["sexy name",": penari","erotic dance"],
	 *
	 * @param jsonInput
	 * @return convertedJsonKeynameOutput
	 */
	public String syntaxConvertJsonKeyNames(String jsonInput) {

		log.trace("before convertJsonKeyNames: " + jsonInput);
		// pre-clean json - rid spaces of element names (misinterpreted as elements with attributes in xml)
		// replace ' 's in JSON Namens with '_'
		while (jsonInput.matches(".*\"([^\"]*)\\s+([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*)\\s+([^\"]*)\":", "\"$1_$2\":");
		}

		// replace forward-slash (sign '/' ) in JSON Names with '_'
		while (jsonInput.matches(".*\"([^\"]*)/([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*)/([^\"]*)\":", "\"$1_$2\":");
		}

		// replace '(' in JSON Names with ''
		while (jsonInput.matches(".*\"([^\"]*)[(]([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*)[(]([^\"]*)\":", "\"$1$2\":");
		}

		// replace ')' in JSON Names with ''
		while (jsonInput.matches(".*\"([^\"]*)[)]([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*)[)]([^\"]*)\":", "\"$1$2\":");
		}

		// add prefix of startNumbers in JSON Keynames with 'n_'
		while (jsonInput.matches(".*\"([^\"][0-9])([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"][0-9])([^\"]*)\":", "\"n_$1$2\":");
		}
		// add prefix of only numbers in JSON Keynames with 'm_'
		while (jsonInput.matches(".*\"([0-9]+)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([0-9]+)\":", "\"m_$1\":");
		}

		// replace ':' between number like '2018-08-28T11:05:00Z' in JSON keynames with ''
		while (jsonInput.matches(".*\"([^\"]*[0-9]):([0-9][^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*[0-9]):([0-9][^\"]*)\":", "\"$1$2\":");
		}

		// replace ',' in JSON Keynames with '.' to prevent , in xml tagnames.
		// while (jsonInput.matches(".*\"([^\"]*),([^\"]*)\":.*")) {
		// jsonInput = jsonInput.replaceAll("\"([^\"]*),([^\"]*)\":", "\"$1.$2\":");
		// }

		// replace '=' in JSON Keynames with '-'
		while (jsonInput.matches(".*\"([^\"]*)=([^\"]*)\":.*")) {
			jsonInput = jsonInput.replaceAll("\"([^\"]*)=([^\"]*)\":", "\"$1-$2\":");
		}

		log.trace("after syntaxConvertJsonKeyNames: " + jsonInput);
		return jsonInput;
	}

	public String convertToXML(final String jsonRecord) {
		String resultXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
		org.json.JSONObject jsonObject = new org.json.JSONObject(syntaxConvertJsonKeyNames(jsonRecord));
		resultXml += org.json.XML.toString(jsonObject, wrapName); // wrap xml in single root element
		log.trace("before inputStream: " + resultXml);
		resultXml = XmlCleaner.cleanAllEntities(resultXml);
		log.trace("after cleaning: " + resultXml);
		return resultXml;
	}
}
