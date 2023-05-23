
package eu.dnetlib.dhp.schema.oaf.utils;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

import java.sql.Array;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.common.AccessRightComparator;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class OafMapperUtils {

	private OafMapperUtils() {
	}

	public static Oaf merge(final Oaf left, final Oaf right) {
		if (ModelSupport.isSubClass(left, OafEntity.class)) {
			return mergeEntities((OafEntity) left, (OafEntity) right);
		} else if (ModelSupport.isSubClass(left, Relation.class)) {
			((Relation) left).mergeFrom((Relation) right);
		} else {
			throw new IllegalArgumentException("invalid Oaf type:" + left.getClass().getCanonicalName());
		}
		return left;
	}

	public static OafEntity mergeEntities(OafEntity left, OafEntity right) {
		if (ModelSupport.isSubClass(left, Result.class)) {
			return mergeResults((Result) left, (Result) right);
		} else if (ModelSupport.isSubClass(left, Datasource.class)) {
			left.mergeFrom(right);
		} else if (ModelSupport.isSubClass(left, Organization.class)) {
			left.mergeFrom(right);
		} else if (ModelSupport.isSubClass(left, Project.class)) {
			left.mergeFrom(right);
		} else {
			throw new IllegalArgumentException("invalid OafEntity subtype:" + left.getClass().getCanonicalName());
		}
		return left;
	}

	public static Result mergeResults(Result left, Result right) {

		final boolean leftFromDelegatedAuthority = isFromDelegatedAuthority(left);
		final boolean rightFromDelegatedAuthority = isFromDelegatedAuthority(right);

		if (leftFromDelegatedAuthority && !rightFromDelegatedAuthority) {
			return left;
		}
		if (!leftFromDelegatedAuthority && rightFromDelegatedAuthority) {
			return right;
		}

		if (new ResultTypeComparator().compare(left, right) < 0) {
			left.mergeFrom(right);
			return left;
		} else {
			right.mergeFrom(left);
			return right;
		}
	}

	private static boolean isFromDelegatedAuthority(Result r) {
		return Optional
			.ofNullable(r.getInstance())
			.map(
				instance -> instance
					.stream()
					.filter(i -> Objects.nonNull(i.getCollectedfrom()))
					.map(i -> i.getCollectedfrom().getKey())
					.anyMatch(cfId -> IdentifierFactory.delegatedAuthorityDatasourceIds().contains(cfId)))
			.orElse(false);
	}

	public static KeyValue keyValue(final String k, final String v) {
		final KeyValue kv = new KeyValue();
		kv.setKey(k);
		kv.setValue(v);
		return kv;
	}

	public static List<KeyValue> listKeyValues(final String... s) {
		if (s.length % 2 > 0) {
			throw new IllegalArgumentException("Invalid number of parameters (k,v,k,v,....)");
		}

		final List<KeyValue> list = new ArrayList<>();
		for (int i = 0; i < s.length; i += 2) {
			list.add(keyValue(s[i], s[i + 1]));
		}
		return list;
	}

	public static <T> Field<T> field(final T value, final DataInfo info) {
		if (value == null || StringUtils.isBlank(value.toString())) {
			return null;
		}

		final Field<T> field = new Field<>();
		field.setValue(value);
		field.setDataInfo(info);
		return field;
	}

	public static List<Field<String>> listFields(final DataInfo info, final String... values) {
		return Arrays
			.stream(values)
			.map(v -> field(v, info))
			.filter(Objects::nonNull)
			.filter(distinctByKey(Field::getValue))
			.collect(Collectors.toList());
	}

	public static <T> List<T> listValues(Array values) throws SQLException {
		if (Objects.isNull(values)) {
			return null;
		}
		return Arrays
			.stream((T[]) values.getArray())
			.filter(Objects::nonNull)
			.distinct()
			.collect(Collectors.toList());
	}

	public static List<Field<String>> listFields(final DataInfo info, final List<String> values) {
		return values
			.stream()
			.map(v -> field(v, info))
			.filter(Objects::nonNull)
			.filter(distinctByKey(Field::getValue))
			.collect(Collectors.toList());
	}

	public static Qualifier unknown(final String schemeid, final String schemename) {
		return qualifier(UNKNOWN, "Unknown", schemeid, schemename);
	}

	public static AccessRight accessRight(
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename) {
		return accessRight(classid, classname, schemeid, schemename, null);
	}

	public static AccessRight accessRight(
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename,
		final OpenAccessRoute openAccessRoute) {
		final AccessRight accessRight = new AccessRight();
		accessRight.setClassid(classid);
		accessRight.setClassname(classname);
		accessRight.setSchemeid(schemeid);
		accessRight.setSchemename(schemename);
		accessRight.setOpenAccessRoute(openAccessRoute);
		return accessRight;
	}

	public static Qualifier qualifier(
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename) {
		final Qualifier q = new Qualifier();
		q.setClassid(classid);
		q.setClassname(classname);
		q.setSchemeid(schemeid);
		q.setSchemename(schemename);
		return q;
	}

	public static Qualifier qualifier(final Qualifier qualifier) {
		final Qualifier q = new Qualifier();
		q.setClassid(qualifier.getClassid());
		q.setClassname(qualifier.getClassname());
		q.setSchemeid(qualifier.getSchemeid());
		q.setSchemename(qualifier.getSchemename());
		return q;
	}

	public static Subject subject(
		final String value,
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename,
		final DataInfo dataInfo) {

		return subject(value, qualifier(classid, classname, schemeid, schemename), dataInfo);
	}

	public static StructuredProperty structuredProperty(
		final String value,
		final String classid,
		final String classname,
		final String schemeid,
		final String schemename,
		final DataInfo dataInfo) {

		return structuredProperty(value, qualifier(classid, classname, schemeid, schemename), dataInfo);
	}

	public static Subject subject(
		final String value,
		final Qualifier qualifier,
		final DataInfo dataInfo) {
		if (value == null) {
			return null;
		}
		final Subject s = new Subject();
		s.setValue(value);
		s.setQualifier(qualifier);
		s.setDataInfo(dataInfo);
		return s;
	}

	public static StructuredProperty structuredProperty(
		final String value,
		final Qualifier qualifier,
		final DataInfo dataInfo) {
		if (value == null) {
			return null;
		}
		final StructuredProperty sp = new StructuredProperty();
		sp.setValue(value);
		sp.setQualifier(qualifier);
		sp.setDataInfo(dataInfo);
		return sp;
	}

	public static ExtraInfo extraInfo(
		final String name,
		final String value,
		final String typology,
		final String provenance,
		final String trust) {
		final ExtraInfo info = new ExtraInfo();
		info.setName(name);
		info.setValue(value);
		info.setTypology(typology);
		info.setProvenance(provenance);
		info.setTrust(trust);
		return info;
	}

	public static OAIProvenance oaiIProvenance(
		final String identifier,
		final String baseURL,
		final String metadataNamespace,
		final Boolean altered,
		final String datestamp,
		final String harvestDate) {

		final OriginDescription desc = new OriginDescription();
		desc.setIdentifier(identifier);
		desc.setBaseURL(baseURL);
		desc.setMetadataNamespace(metadataNamespace);
		desc.setAltered(altered);
		desc.setDatestamp(datestamp);
		desc.setHarvestDate(harvestDate);

		final OAIProvenance p = new OAIProvenance();
		p.setOriginDescription(desc);

		return p;
	}

	public static Journal journal(
		final String name,
		final String issnPrinted,
		final String issnOnline,
		final String issnLinking,
		final DataInfo dataInfo) {

		return hasIssn(issnPrinted, issnOnline, issnLinking) ? journal(
			name,
			issnPrinted,
			issnOnline,
			issnLinking,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			dataInfo) : null;
	}

	public static Journal journal(
		final String name,
		final String issnPrinted,
		final String issnOnline,
		final String issnLinking,
		final String ep,
		final String iss,
		final String sp,
		final String vol,
		final String edition,
		final String conferenceplace,
		final String conferencedate,
		final DataInfo dataInfo) {

		if (StringUtils.isNotBlank(name) || hasIssn(issnPrinted, issnOnline, issnLinking)) {
			final Journal j = new Journal();
			j.setName(name);
			j.setIssnPrinted(issnPrinted);
			j.setIssnOnline(issnOnline);
			j.setIssnLinking(issnLinking);
			j.setEp(ep);
			j.setIss(iss);
			j.setSp(sp);
			j.setVol(vol);
			j.setEdition(edition);
			j.setConferenceplace(conferenceplace);
			j.setConferencedate(conferencedate);
			j.setDataInfo(dataInfo);
			return j;
		} else {
			return null;
		}
	}

	private static boolean hasIssn(String issnPrinted, String issnOnline, String issnLinking) {
		return StringUtils.isNotBlank(issnPrinted)
			|| StringUtils.isNotBlank(issnOnline)
			|| StringUtils.isNotBlank(issnLinking);
	}

	public static DataInfo dataInfo(
		final Boolean deletedbyinference,
		final String inferenceprovenance,
		final Boolean inferred,
		final Boolean invisible,
		final Qualifier provenanceaction,
		final String trust) {
		final DataInfo d = new DataInfo();
		d.setDeletedbyinference(deletedbyinference);
		d.setInferenceprovenance(inferenceprovenance);
		d.setInferred(inferred);
		d.setInvisible(invisible);
		d.setProvenanceaction(provenanceaction);
		d.setTrust(trust);
		return d;
	}

	public static String createOpenaireId(
		final int prefix,
		final String originalId,
		final boolean to_md5) {
		if (StringUtils.isBlank(originalId)) {
			return null;
		} else if (to_md5) {
			final String nsPrefix = StringUtils.substringBefore(originalId, "::");
			final String rest = StringUtils.substringAfter(originalId, "::");
			return String.format("%s|%s::%s", prefix, nsPrefix, IdentifierFactory.md5(rest));
		} else {
			return String.format("%s|%s", prefix, originalId);
		}
	}

	public static String createOpenaireId(
		final String type,
		final String originalId,
		final boolean to_md5) {
		switch (type) {
			case "datasource":
				return createOpenaireId(10, originalId, to_md5);
			case "organization":
				return createOpenaireId(20, originalId, to_md5);
			case "person":
				return createOpenaireId(30, originalId, to_md5);
			case "project":
				return createOpenaireId(40, originalId, to_md5);
			default:
				return createOpenaireId(50, originalId, to_md5);
		}
	}

	public static String asString(final Object o) {
		return o == null ? "" : o.toString();
	}

	public static <T> Predicate<T> distinctByKey(
		final Function<? super T, ?> keyExtractor) {
		final Map<Object, Boolean> seen = new ConcurrentHashMap<>();
		return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
	}

	public static Qualifier createBestAccessRights(final List<Instance> instanceList) {
		return getBestAccessRights(instanceList);
	}

	protected static Qualifier getBestAccessRights(final List<Instance> instanceList) {
		if (instanceList != null) {
			final Optional<AccessRight> min = instanceList
				.stream()
				.map(Instance::getAccessright)
				.min(new AccessRightComparator<>());

			final Qualifier rights = min.map(OafMapperUtils::qualifier).orElseGet(Qualifier::new);

			if (StringUtils.isBlank(rights.getClassid())) {
				rights.setClassid(UNKNOWN);
			}
			if (StringUtils.isBlank(rights.getClassname())
				|| UNKNOWN.equalsIgnoreCase(rights.getClassname())) {
				rights.setClassname(NOT_AVAILABLE);
			}
			if (StringUtils.isBlank(rights.getSchemeid())) {
				rights.setSchemeid(DNET_ACCESS_MODES);
			}
			if (StringUtils.isBlank(rights.getSchemename())) {
				rights.setSchemename(DNET_ACCESS_MODES);
			}

			return rights;
		}
		return null;
	}

	public static KeyValue newKeyValueInstance(String key, String value, DataInfo dataInfo) {
		KeyValue kv = new KeyValue();
		kv.setDataInfo(dataInfo);
		kv.setKey(key);
		kv.setValue(value);
		return kv;
	}

	public static Measure newMeasureInstance(String id, String value, String key, DataInfo dataInfo) {
		Measure m = new Measure();
		m.setId(id);
		m.setUnit(Arrays.asList(newKeyValueInstance(key, value, dataInfo)));
		return m;
	}

	public static Relation getRelation(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final OafEntity entity) {
		return getRelation(source, target, relType, subRelType, relClass, entity, null);
	}

	public static Relation getRelation(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final OafEntity entity,
		final String validationDate) {
		return getRelation(
			source, target, relType, subRelType, relClass, entity.getCollectedfrom(), entity.getDataInfo(),
			entity.getLastupdatetimestamp(), validationDate, null);
	}

	public static Relation getRelation(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final List<KeyValue> collectedfrom,
		final DataInfo dataInfo,
		final Long lastupdatetimestamp) {
		return getRelation(
			source, target, relType, subRelType, relClass, collectedfrom, dataInfo, lastupdatetimestamp, null, null);
	}

	public static Relation getRelation(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final List<KeyValue> collectedfrom,
		final DataInfo dataInfo,
		final Long lastupdatetimestamp,
		final String validationDate,
		final List<KeyValue> properties) {
		final Relation rel = new Relation();
		rel.setRelType(relType);
		rel.setSubRelType(subRelType);
		rel.setRelClass(relClass);
		rel.setSource(source);
		rel.setTarget(target);
		rel.setCollectedfrom(collectedfrom);
		rel.setDataInfo(dataInfo);
		rel.setLastupdatetimestamp(lastupdatetimestamp);
		rel.setValidated(StringUtils.isNotBlank(validationDate));
		rel.setValidationDate(StringUtils.isNotBlank(validationDate) ? validationDate : null);
		rel.setProperties(properties);
		return rel;
	}

	public static String getProvenance(DataInfo dataInfo) {
		return Optional
			.ofNullable(dataInfo)
			.map(
				d -> Optional
					.ofNullable(d.getProvenanceaction())
					.map(Qualifier::getClassid)
					.orElse(""))
			.orElse("");
	}
}
