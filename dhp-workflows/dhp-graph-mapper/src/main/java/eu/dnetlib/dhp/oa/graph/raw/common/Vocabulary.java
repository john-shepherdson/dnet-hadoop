
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.util.HashMap;
import java.util.Map;

public class Vocabulary {

	private final String id;
	private final String name;

	private final Map<String, VocabularyTerm> terms = new HashMap<>();

	public Vocabulary(final String id, final String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	protected Map<String, VocabularyTerm> getTerms() {
		return terms;
	}

	public VocabularyTerm getTerm(final String id) {
		return terms.get(id.toLowerCase());
	}

	protected void addTerm(final String id, final String name) {
		terms.put(id.toLowerCase(), new VocabularyTerm(id, name));
	}

	protected boolean termExists(final String id) {
		return terms.containsKey(id.toLowerCase());
	}
}
