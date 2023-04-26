
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

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.common.AccessRightComparator;

public class OafMapperUtils {

    private OafMapperUtils() {
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

    public static Qualifier unknown(final String schemeid) {
        return qualifier(UNKNOWN, "Unknown", schemeid);
    }

    public static AccessRight accessRight(
            final String classid,
            final String classname,
            final String schemeid) {
        return accessRight(classid, classname, schemeid, null);
    }

    public static AccessRight accessRight(
            final String classid,
            final String classname,
            final String schemeid,
            final OpenAccessRoute openAccessRoute) {
        final AccessRight accessRight = new AccessRight();
        accessRight.setClassid(classid);
        accessRight.setClassname(classname);
        accessRight.setSchemeid(schemeid);
        accessRight.setOpenAccessRoute(openAccessRoute);
        return accessRight;
    }

    public static Qualifier qualifier(
            final String classid,
            final String classname,
            final String schemeid) {
        final Qualifier q = new Qualifier();
        q.setClassid(classid);
        q.setClassname(classname);
        q.setSchemeid(schemeid);
        return q;
    }

    public static Qualifier qualifier(final Qualifier qualifier) {
        final Qualifier q = new Qualifier();
        q.setClassid(qualifier.getClassid());
        q.setClassname(qualifier.getClassname());
        q.setSchemeid(qualifier.getSchemeid());
        return q;
    }

    public static Subject subject(
            final String value,
            final String classid,
            final String classname,
            final String schemeid,
            final DataInfo dataInfo) {

        return subject(value, qualifier(classid, classname, schemeid), dataInfo);
    }

    public static StructuredProperty structuredProperty(
            final String value,
            final String classid,
            final String classname,
            final String schemeid) {

        return structuredProperty(value, qualifier(classid, classname, schemeid));
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
            final Qualifier qualifier) {
        if (value == null) {
            return null;
        }
        final StructuredProperty sp = new StructuredProperty();
        sp.setValue(value);
        sp.setQualifier(qualifier);
        return sp;
    }

    public static Publisher publisher(final String name) {
        final Publisher p = new Publisher();
        p.setName(name);
        return p;
    }

    public static License license(final String url) {
        final License l = new License();
        l.setUrl(url);
        return l;
    }

    public static AuthorPid authorPid(
            final String value,
            final Qualifier qualifier,
            final DataInfo dataInfo) {
        if (value == null) {
            return null;
        }
        final AuthorPid ap = new AuthorPid();
        ap.setValue(value);
        ap.setQualifier(qualifier);
        ap.setDataInfo(dataInfo);
        return ap;
    }

    public static AuthorPid authorPid(
            final String value,
            final String classid,
            final String schemeid,
            final DataInfo dataInfo) {
        if (value == null) {
            return null;
        }
        final AuthorPid ap = new AuthorPid();
        ap.setValue(value);
        ap.setQualifier(qualifier(classid, classid, schemeid));
        ap.setDataInfo(dataInfo);
        return ap;
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
            final String issnLinking) {

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
                null) : null;
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
            final String conferencedate) {

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
            final float trust,
            final String inferenceprovenance,
            final boolean inferred,
            final Qualifier provenanceaction) {
        final DataInfo d = new DataInfo();
        d.setTrust(trust);
        d.setInferenceprovenance(inferenceprovenance);
        d.setInferred(inferred);
        d.setProvenanceaction(provenanceaction);
        return d;
    }

    public static EntityDataInfo dataInfo(
            final boolean invisible,
            final boolean deletedbyinference,
            final float trust,
            final String inferenceprovenance,
            final boolean inferred,
            final Qualifier provenanceaction) {
        final EntityDataInfo d = new EntityDataInfo();
        d.setTrust(trust);
        d.setInvisible(invisible);
        d.setDeletedbyinference(deletedbyinference);
        d.setInferenceprovenance(inferenceprovenance);
        d.setInferred(inferred);
        d.setProvenanceaction(provenanceaction);
        return d;
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

            return rights;
        }
        return null;
    }

    public static Measure newMeasureInstance(String id, String value, String key, DataInfo dataInfo) {
        Measure m = new Measure();
        m.setId(id);
        m.setUnit(Arrays.asList(unit(key, value, dataInfo)));
        return m;
    }

    public static MeasureUnit unit(String key, String value, DataInfo dataInfo) {
        MeasureUnit unit = new MeasureUnit();
        unit.setKey(key);
        unit.setValue(value);
        unit.setDataInfo(dataInfo);
        return unit;
    }

    public static Relation getRelation(final String source,
                                       final String target,
                                       final Relation.RELTYPE relType,
                                       final Relation.SUBRELTYPE subRelType,
                                       final Relation.RELCLASS relClass,
                                       final Entity entity) {
        return getRelation(source, target, relType, subRelType, relClass, entity, null);
    }

    public static Relation getRelation(final String source,
                                       final String target,
                                       final Relation.RELTYPE relType,
                                       final Relation.SUBRELTYPE subRelType,
                                       final Relation.RELCLASS relClass,
                                       final Entity entity,
                                       final String validationDate) {

        final List<Provenance> provenance = getProvenance(
                entity.getCollectedfrom(), fromEntityDataInfo(entity.getDataInfo()));
        return getRelation(
                source, target, relType, subRelType, relClass, provenance, validationDate, null);
    }

    public static Relation getRelation(final String source,
                                       final String target,
                                       final Relation.RELTYPE relType,
                                       final Relation.SUBRELTYPE subRelType,
                                       final Relation.RELCLASS relClass,
                                       final List<Provenance> provenance) {
        return getRelation(
                source, target, relType, subRelType, relClass, provenance, null, null);
    }

    public static Relation getRelation(final String source,
                                       final String target,
                                       final Relation.RELTYPE relType,
                                       final Relation.SUBRELTYPE subRelType,
                                       final Relation.RELCLASS relClass,
                                       final List<Provenance> provenance,
                                       final List<KeyValue> properties) {
        return getRelation(
                source, target, relType, subRelType, relClass, provenance, null, properties);
    }

    public static Relation getRelation(final String source,
                                       final String target,
                                       final Relation.RELTYPE relType,
                                       final Relation.SUBRELTYPE subRelType,
                                       final Relation.RELCLASS relClass,
                                       final List<Provenance> provenance,
                                       final String validationDate,
                                       final List<KeyValue> properties) {
        final Relation rel = new Relation();
        rel.setRelType(relType);
        rel.setSubRelType(subRelType);
        rel.setRelClass(relClass);
        rel.setSource(source);
        rel.setTarget(target);
        rel.setProvenance(provenance);
        rel.setValidated(StringUtils.isNotBlank(validationDate));
        rel.setValidationDate(StringUtils.isNotBlank(validationDate) ? validationDate : null);
        rel.setProperties(properties);
        return rel;
    }

    public static List<Provenance> getProvenance(final List<KeyValue> collectedfrom, final DataInfo dataInfo) {
        return collectedfrom
                .stream()
                .map(cf -> getProvenance(cf, dataInfo))
                .collect(Collectors.toList());
    }

    public static Provenance getProvenance(final KeyValue collectedfrom, final DataInfo dataInfo) {
        final Provenance prov = new Provenance();
        prov.setCollectedfrom(collectedfrom);
        prov.setDataInfo(dataInfo);
        return prov;
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

    public static DataInfo fromEntityDataInfo(EntityDataInfo entityDataInfo) {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setTrust(entityDataInfo.getTrust());
        dataInfo.setInferenceprovenance(entityDataInfo.getInferenceprovenance());
        dataInfo.setInferred(entityDataInfo.getInferred());
        dataInfo.setProvenanceaction(entityDataInfo.getProvenanceaction());
        return dataInfo;
    }
}
