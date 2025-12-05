
package eu.dnetlib.dhp.common.person;

import static eu.dnetlib.dhp.common.person.Constants.*;

import java.util.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class CoAuthorshipIterator implements Iterator<CoAuthorship> {
	private int firstIndex;
	private int secondIndex;
	private boolean firstRelation;
	private List<String> authors;

	@Override
	public boolean hasNext() {
		return firstIndex < authors.size() - 1;
	}

	@Override
	public CoAuthorship next() {
		CoAuthorship rel = null;
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
		this.authors = new ArrayList<>(new HashSet<>(authors));
		this.firstIndex = 0;
		this.secondIndex = 1;
		this.firstRelation = Boolean.TRUE;

	}

	private CoAuthorship getRelation(String orcid1, String orcid2) {
		CoAuthorship coAuthorship = new CoAuthorship();

		String source = Constants.getPersonId(orcid1);
		String target = Constants.getPersonId(orcid2);
		coAuthorship.setAuthor1(source);
		coAuthorship.setAuthor2(target);
		coAuthorship.setCoauthoredProducts(1);
		return coAuthorship;
	}
}
