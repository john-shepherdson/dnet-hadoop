
package eu.dnetlib.dhp.actionmanager.personentity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;

public class CoAuthorshipIterator implements Iterator<Relation> {
	private int firstIndex;
	private int secondIndex;
	private boolean firstRelation;
	private List<String> authors;
	private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______::";
	private static final String OPENAIRE_PREFIX = "openaire____";
	private static final String SEPARATOR = "::";
	private static final String ORCID_KEY = "10|" + OPENAIRE_PREFIX + SEPARATOR
		+ DHPUtils.md5(ModelConstants.ORCID.toLowerCase());
	public static final String ORCID_AUTHORS_CLASSID = "sysimport:crosswalk:orcid";
	public static final String ORCID_AUTHORS_CLASSNAME = "Imported from ORCID";

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
		Relation relation =
		OafMapperUtils
			.getRelation(
				source, target, ModelConstants.PERSON_PERSON_RELTYPE,
				ModelConstants.PERSON_PERSON_SUBRELTYPE,
				ModelConstants.PERSON_PERSON_HASCOAUTHORED,
				Arrays.asList(OafMapperUtils.keyValue(ORCID_KEY, ModelConstants.ORCID_DS)),
				OafMapperUtils
					.dataInfo(
						false, null, false, false,
						OafMapperUtils
							.qualifier(
								ORCID_AUTHORS_CLASSID, ORCID_AUTHORS_CLASSNAME,
								ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
						"0.91"),
				null);
		relation.setValidated(true);
		return relation;
	}
}
