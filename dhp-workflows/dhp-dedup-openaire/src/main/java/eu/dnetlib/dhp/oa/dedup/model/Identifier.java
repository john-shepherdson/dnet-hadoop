
package eu.dnetlib.dhp.oa.dedup.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.oa.dedup.DatePicker;
import eu.dnetlib.dhp.oa.dedup.IdentifierComparator;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class Identifier<T extends OafEntity> implements Serializable, Comparable<Identifier<T>> {

	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String BASE_DATE = "2000-01-01";

	private T entity;

	// cached date value
	private Date date = null;

	public static <T extends OafEntity> Identifier<T> newInstance(T entity) {
		return new Identifier<>(entity);
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
			String sDate = LocalDate.now().plusDays(1).toString();
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

	public PidType getPidType() {
		return PidType.tryValueOf(StringUtils.substringBefore(StringUtils.substringAfter(entity.getId(), "|"), "_"));
	}

	@Override
	public int compareTo(Identifier<T> i) {
		return IdentifierComparator.compareIdentifiers(this, i);
	}
}
