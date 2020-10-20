
package eu.dnetlib.dhp.oa.dedup;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.dedup.model.PidType;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;

public class IdGenerator implements Serializable {

	public static String CROSSREF_ID = "10|openaire____::081b82f96300b6a6e3d282bad31cb6e2";
	public static String DATACITE_ID = "10|openaire____::9e3be59865b2c1c335d32dae2fe7b254";
	public static String BASE_DATE = "2000-01-01";

	// pick the best pid from the list (consider date and pidtype)
	public static String generate(List<Identifier> pids, String defaultID) {
		if (pids == null || pids.size() == 0)
			return defaultID;

		Optional<Identifier> bp = pids
			.stream()
			.max(Identifier::compareTo);

		if (bp.get().isUseOriginal() || bp.get().getPid().getValue() == null) {
			return bp.get().getOriginalID().split("\\|")[0] + "|dedup_wf_001::"
				+ DHPUtils.md5(bp.get().getOriginalID());
		} else {
			return bp.get().getOriginalID().split("\\|")[0] + "|"
				+ createPrefix(bp.get().getPid().getQualifier().getClassid()) + "::"
				+ DHPUtils.md5(bp.get().getPid().getValue());
		}

	}

	public static <T extends OafEntity> ArrayList<Identifier> createBasePid(T entity, SimpleDateFormat sdf) {

		Date date;
		try {
			date = sdf.parse(BASE_DATE);
		} catch (ParseException e) {
			date = new Date();
		}
		return Lists
			.newArrayList(
				new Identifier(new StructuredProperty(), date, PidType.original, entity.getCollectedfrom(),
					EntityType.fromClass(entity.getClass()), entity.getId()));
	}

	// pick the best pid from the entity. Returns a list (length 1) to save time in the call
	public static <T extends OafEntity> List<Identifier> bestPidToIdentifier(T entity) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		if (entity.getPid() == null || entity.getPid().size() == 0)
			return createBasePid(entity, sdf);

		Optional<StructuredProperty> bp = entity
			.getPid()
			.stream()
			.filter(pid -> PidType.classidValueOf(pid.getQualifier().getClassid()) != PidType.undefined)
			.max(Comparator.comparing(pid -> PidType.classidValueOf(pid.getQualifier().getClassid())));

		return bp
			.map(
				structuredProperty -> Lists
					.newArrayList(
						new Identifier(structuredProperty, extractDate(entity, sdf),
							PidType.classidValueOf(structuredProperty.getQualifier().getClassid()),
							entity.getCollectedfrom(), EntityType.fromClass(entity.getClass()), entity.getId())))
			.orElseGet(() -> createBasePid(entity, sdf));

	}

	// create the prefix (length = 12): dedup_+ pidType
	public static String createPrefix(String pidType) {

		StringBuilder prefix = new StringBuilder("dedup_" + pidType);

		while (prefix.length() < 12) {
			prefix.append("_");
		}
		return prefix.toString().substring(0, 12);

	}

	// extracts the date from the record. If the date is not available or is not wellformed, it returns a base date:
	// 00-01-01
	public static <T extends OafEntity> Date extractDate(T duplicate, SimpleDateFormat sdf) {

		String date = BASE_DATE;
		if (ModelSupport.isSubClass(duplicate, Result.class)) {
			Result result = (Result) duplicate;
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

	public static boolean isWellformed(Field<String> date) {
		return date != null && StringUtils.isNotBlank(date.getValue())
			&& date.getValue().matches(DatePicker.DATE_PATTERN) && DatePicker.inRange(date.getValue());
	}
}
