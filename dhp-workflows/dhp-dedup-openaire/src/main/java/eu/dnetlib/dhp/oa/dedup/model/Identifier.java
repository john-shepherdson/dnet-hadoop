
package eu.dnetlib.dhp.oa.dedup.model;

import com.google.common.collect.Sets;
import eu.dnetlib.dhp.oa.dedup.DatePicker;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.PidComparator;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Identifier<T extends OafEntity> implements Serializable, Comparable<Identifier> {

	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String BASE_DATE = "2000-01-01";

	private T entity;

	// cached date value
	private Date date = null;

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
		if (Objects.nonNull(date)) {
			return date;
		} else {
			String sDate = BASE_DATE;
			if (ModelSupport.isSubClass(getEntity(), Result.class)) {
				Result result = (Result) getEntity();
				if (isWellformed(result.getDateofacceptance())) {
					sDate = result.getDateofacceptance().getValue();
				}
			}
			try {
				this.date = new SimpleDateFormat(DATE_FORMAT).parse(sDate);
				return date;
			} catch (Throwable e) {
				throw new RuntimeException(
					String.format("cannot parse date: '%s' from record: '%s'", sDate, entity.getId()));
			}
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
				if (isFromDatasourceID(lKeys, ModelConstants.CROSSREF_ID)
					&& !isFromDatasourceID(rKeys, ModelConstants.CROSSREF_ID))
					return -1;
				if (isFromDatasourceID(rKeys, ModelConstants.CROSSREF_ID)
					&& !isFromDatasourceID(lKeys, ModelConstants.CROSSREF_ID))
					return 1;
			}
			if (getEntityType() == EntityType.dataset) {
				if (isFromDatasourceID(lKeys, ModelConstants.DATACITE_ID)
					&& !isFromDatasourceID(rKeys, ModelConstants.DATACITE_ID))
					return -1;
				if (isFromDatasourceID(rKeys, ModelConstants.DATACITE_ID)
					&& !isFromDatasourceID(lKeys, ModelConstants.DATACITE_ID))
					return 1;
			}

			if (this.getDate().compareTo(i.getDate()) == 0) {// same date
				// we need to take the alphabetically lower id
				return this.getOriginalID().compareTo(i.getOriginalID());
			} else
				// we need to take the elder date
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
