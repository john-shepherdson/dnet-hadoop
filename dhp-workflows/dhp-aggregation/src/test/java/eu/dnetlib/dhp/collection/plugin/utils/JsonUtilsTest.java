
package eu.dnetlib.dhp.collection.plugin.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JsonUtilsTest {

	static private String wrapped(String xml) {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><recordWrap>" + xml + "</recordWrap>";
	}

	@Test
	void keyStartWithDigit() {
		assertEquals(
			wrapped("<m_100><n_200v>null</n_200v></m_100>"),
			JsonUtils.convertToXML("{\"100\" : {\"200v\" : null}}"));
	}

	@Test
	void keyStartWithSpecialchars() {
		assertEquals(
			wrapped("<_parent><_nest1><_nest2>null</_nest2></_nest1></_parent>"),
			JsonUtils.convertToXML("{\"   parent\" : {\"-nest1\" : {\".nest2\" : null}}}"));
	}

	@Test
	void encodeArray() {
		assertEquals(
			wrapped("<_parent.child>1</_parent.child><_parent.child>2</_parent.child>"),
			JsonUtils.convertToXML("{\" parent.child\":[1, 2]}"));
	}

	@Test
	void arrayOfObjects() {
		assertEquals(
			wrapped("<parent><id>1</id></parent><parent><id>2</id></parent>"),
			JsonUtils.convertToXML("{\"parent\": [{\"id\": 1}, {\"id\": 2}]}"));
	}

	@Test
	void removeControlCharacters() {
		assertEquals(
			wrapped("<m_100><n_200v>Test</n_200v></m_100>"),
			JsonUtils.convertToXML("{\"100\" : {\"200v\" : \"\\u0000\\u000cTest\"}}"));
	}
}
