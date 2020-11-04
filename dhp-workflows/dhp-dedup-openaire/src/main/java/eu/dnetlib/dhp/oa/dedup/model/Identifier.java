
package eu.dnetlib.dhp.oa.dedup.model;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Sets;

import eu.dnetlib.dhp.oa.dedup.DatePicker;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.PidComparator;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class Identifier<T extends OafEntity> implements Serializable, Comparable<Identifier> {

	public static String CROSSREF_ID = "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2";
	public static String DATACITE_ID = "10|openaire____::9e3be59865b2c1c335d32dae2fe7b254";
	public static String BASE_DATE = "2000-01-01";

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private T entity;

	public static <T extends OafEntity> Identifier newInstance(T entity) {
		return new Identifier(entity);
	}

	public Identifier(T entity) {
		this.entity = entity;
	}

	public T getEntity() {
		return entity;
	}

	public void setEntity(T entity) {
		this.entity = entity;
	}

	public Date getDate() {
		String date = BASE_DATE;
		if (ModelSupport.isSubClass(getEntity(), Result.class)) {
			Result result = (Result) getEntity();
			if (isWellformed(result.getDateofacceptance())) {
				date = result.getDateofacceptance().getValue();
			}
		}
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			return new Date();
		}
	}

	private static boolean isWellformed(Field<String> date) {
		return date != null && StringUtils.isNotBlank(date.getValue())
			&& date.getValue().matches(DatePicker.DATE_PATTERN) && DatePicker.inRange(date.getValue());
	}

	public List<KeyValue> getCollectedFrom() {
		return entity.getCollectedfrom();
	}

	public EntityType getEntityType() {
		return EntityType.fromClass(entity.getClass());
	}

	public String getOriginalID() {
		return entity.getId();
	}

	private PidType getPidType() {
		return PidType.tryValueOf(StringUtils.substringBefore(StringUtils.substringAfter(entity.getId(), "|"), "_"));
	}

	@Override
	public int compareTo(Identifier i) {
		// priority in comparisons: 1) pidtype, 2) collectedfrom (depending on the entity type) , 3) date 4)
		// alphabetical order of the originalID

		Set<String> lKeys = Optional
			.ofNullable(getCollectedFrom())
			.map(c -> c.stream().map(KeyValue::getKey).collect(Collectors.toSet()))
			.orElse(Sets.newHashSet());

		final Optional<List<KeyValue>> cf = Optional.ofNullable(i.getCollectedFrom());
		Set<String> rKeys = cf
			.map(c -> c.stream().map(KeyValue::getKey).collect(Collectors.toSet()))
			.orElse(Sets.newHashSet());

		if (this.getPidType().compareTo(i.getPidType()) == 0) { // same type
			if (getEntityType() == EntityType.publication) {
				if (isFromDatasourceID(lKeys, CROSSREF_ID)
					&& !isFromDatasourceID(rKeys, CROSSREF_ID))
					return -1;
				if (isFromDatasourceID(rKeys, CROSSREF_ID)
					&& !isFromDatasourceID(lKeys, CROSSREF_ID))
					return 1;
			}
			if (getEntityType() == EntityType.dataset) {
				if (isFromDatasourceID(lKeys, DATACITE_ID)
					&& !isFromDatasourceID(rKeys, DATACITE_ID))
					return -1;
				if (isFromDatasourceID(rKeys, DATACITE_ID)
					&& !isFromDatasourceID(lKeys, DATACITE_ID))
					return 1;
			}

			if (this.getDate().compareTo(i.getDate()) == 0) {// same date
				// the minus because we need to take the alphabetically lower id
				return this.getOriginalID().compareTo(i.getOriginalID());
			} else
				// the minus is because we need to take the elder date
				return this.getDate().compareTo(i.getDate());
		} else {
			return new PidComparator<>(getEntity()).compare(toSP(getPidType()), toSP(i.getPidType()));
		}

	}

	private StructuredProperty toSP(PidType pidType) {
		return OafMapperUtils.structuredProperty("", pidType.toString(), pidType.toString(), "", "", new DataInfo());
	}

	public boolean isFromDatasourceID(Set<String> collectedFrom, String dsId) {
		return collectedFrom.contains(dsId);
	}
}
