
package eu.dnetlib.dhp.common.vocabulary;

import java.io.Serializable;

public class VocabularyTerm implements Serializable {

	private final String id;
	private final String name;

	public VocabularyTerm(final String id, final String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

}
