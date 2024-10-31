
package eu.dnetlib.dhp.common.person;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;



import static eu.dnetlib.dhp.common.person.Constants.*;

public class CoAuthorshipIterator implements Iterator<Relation> {
	private int firstIndex;
	private int secondIndex;
	private boolean firstRelation;
	private List<String> authors;
	

	@Override
	public boolean hasNext() {
		return firstIndex < authors.size() - 1;
	}

	@Override
	public Relation next() {
		Relation rel = null;
		if (firstRelation) {
			rel = getRelation(authors.get(firstIndex), authors.get(secondIndex));
			firstRelation = Boolean.FALSE;
		} else {
			rel = getRelation(authors.get(secondIndex), authors.get(firstIndex));
			firstRelation = Boolean.TRUE;
			secondIndex += 1;
			if (secondIndex >= authors.size()) {
				firstIndex += 1;
				secondIndex = firstIndex + 1;
			}
		}

		return rel;
	}

	public CoAuthorshipIterator(List<String> authors) {
		this.authors = authors;
		this.firstIndex = 0;
		this.secondIndex = 1;
		this.firstRelation = Boolean.TRUE;

	}

	private Relation getRelation(String orcid1, String orcid2) {
		String source = PERSON_PREFIX + IdentifierFactory.md5(orcid1);
		String target = PERSON_PREFIX + IdentifierFactory.md5(orcid2);
		Relation relation = OafMapperUtils
			.getRelation(
				source, target, ModelConstants.PERSON_PERSON_RELTYPE,
				ModelConstants.PERSON_PERSON_SUBRELTYPE,
				ModelConstants.PERSON_PERSON_HASCOAUTHORED,
                    Collections.singletonList(OafMapperUtils.keyValue(ORCID_KEY, ModelConstants.ORCID_DS)),
				ORCIDDATAINFO,
				null);
		relation.setValidated(true);
		return relation;
	}
}
