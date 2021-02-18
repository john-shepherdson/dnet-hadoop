
package eu.dnetlib.dhp.transformation.vocabulary;

import java.io.Serializable;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * VocabularyHelper
 * 
 * used names of vocabulary in dnet45 transformation
 * detailed information at https://issue.openaire.research-infrastructures.eu/projects/openaire/wiki/Transformation_Rule_Language :
 * "AccessRights":
 * 	 {"name":"dnet:access_modes", "caseSensitive":"false"}, 
 * "Languages":
 *   {"name":"dnet:languages", "caseSensitive":"false", "delimiter":"/"}, 
 * "TextTypologies":
 *   {"name":"dnet:publication_resource", "caseSensitive":"false"}, 
 * "SuperTypes":
 *   {"name":"dnet:result_typologies", "caseSensitive":"false"}, 
 * "ReviewLevels":
 *   {"name":"dnet:review_levels", "caseSensitive":"false"}, 
 * "Countries":
 *   {"name":"dnet:countries", "caseSensitive":"false"}
 * 
 */

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
