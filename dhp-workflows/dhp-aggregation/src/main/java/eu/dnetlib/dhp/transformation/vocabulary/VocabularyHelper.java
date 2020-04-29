
package eu.dnetlib.dhp.transformation.vocabulary;

import java.io.Serializable;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VocabularyHelper implements Serializable {

	private static final String OPENAIRE_URL = "http://api.openaire.eu/vocabularies/%s.json";

	public static Vocabulary getVocabularyFromAPI(final String vocabularyName) throws Exception {
		final URL url = new URL(String.format(OPENAIRE_URL, vocabularyName));

		final String response = IOUtils.toString(url, Charset.defaultCharset());
		final ObjectMapper jsonMapper = new ObjectMapper();
		final Vocabulary vocabulary = jsonMapper.readValue(response, Vocabulary.class);
		return vocabulary;
	}
}
