
package eu.dnetlib.dhp.oa.dedup;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidComparator;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class IdentifierComparator<T extends OafEntity> implements Comparator<Identifier<T>> {

	public static int compareIdentifiers(Identifier left, Identifier right) {
		return new IdentifierComparator<>().compare(left, right);
	}

	@Override
	public int compare(Identifier<T> left, Identifier<T> i) {
		// priority in comparisons: 1) pidtype, 2) collectedfrom (depending on the entity type) , 3) date 4)
		// alphabetical order of the originalID

		Set<String> lKeys = Optional
			.ofNullable(left.getCollectedFrom())
			.map(c -> c.stream().map(KeyValue::getKey).collect(Collectors.toSet()))
			.orElse(Sets.newHashSet());

		final Optional<List<KeyValue>> cf = Optional.ofNullable(i.getCollectedFrom());
		Set<String> rKeys = cf
			.map(c -> c.stream().map(KeyValue::getKey).collect(Collectors.toSet()))
			.orElse(Sets.newHashSet());

		if (left.getPidType().compareTo(i.getPidType()) == 0) { // same type
			if (left.getEntityType() == EntityType.publication) {
				if (isFromDatasourceID(lKeys, ModelConstants.CROSSREF_ID)
					&& !isFromDatasourceID(rKeys, ModelConstants.CROSSREF_ID))
					return -1;
				if (isFromDatasourceID(rKeys, ModelConstants.CROSSREF_ID)
					&& !isFromDatasourceID(lKeys, ModelConstants.CROSSREF_ID))
					return 1;
			}
			if (left.getEntityType() == EntityType.dataset) {
				if (isFromDatasourceID(lKeys, ModelConstants.DATACITE_ID)
					&& !isFromDatasourceID(rKeys, ModelConstants.DATACITE_ID))
					return -1;
				if (isFromDatasourceID(rKeys, ModelConstants.DATACITE_ID)
					&& !isFromDatasourceID(lKeys, ModelConstants.DATACITE_ID))
					return 1;
			}

			if (left.getDate().compareTo(i.getDate()) == 0) {// same date
				// we need to take the alphabetically lower id
				return left.getOriginalID().compareTo(i.getOriginalID());
			} else
				// we need to take the elder date
				return left.getDate().compareTo(i.getDate());
		} else {
			return new PidComparator<>(left.getEntity()).compare(toSP(left.getPidType()), toSP(i.getPidType()));
		}
	}

	public boolean isFromDatasourceID(Set<String> collectedFrom, String dsId) {
		return collectedFrom.contains(dsId);
	}

	private StructuredProperty toSP(PidType pidType) {
		return OafMapperUtils.structuredProperty("", pidType.toString(), pidType.toString(), "", "", new DataInfo());
	}

}
