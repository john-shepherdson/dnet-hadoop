package eu.dnetlib.dhp.graph.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.graph.model.EntityRelEntity;
import eu.dnetlib.dhp.graph.model.RelatedEntity;
import eu.dnetlib.dhp.graph.model.TypedRow;
import eu.dnetlib.dhp.schema.oaf.*;
import net.minidev.json.JSONArray;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.*;

public class GraphMappingUtils {

    public static final String SEPARATOR = "_";

    public enum EntityType {
        publication, dataset, otherresearchproduct, software, datasource, organization, project
    }

    public enum MainEntityType {
        result, datasource, organization, project
    }

    public static Set<String> authorPidTypes = Sets.newHashSet("orcid", "magidentifier");

    public static Set<String> instanceFieldFilter = Sets.newHashSet("instancetype", "hostedby", "license", "accessright", "collectedfrom", "dateofacceptance", "distributionlocation");

    private static final String schemeTemplate = "dnet:%s_%s_relations";

    private static Map<EntityType, MainEntityType> entityMapping = Maps.newHashMap();

    static {
        entityMapping.put(EntityType.publication,            MainEntityType.result);
        entityMapping.put(EntityType.dataset,                MainEntityType.result);
        entityMapping.put(EntityType.otherresearchproduct,   MainEntityType.result);
        entityMapping.put(EntityType.software,               MainEntityType.result);
        entityMapping.put(EntityType.datasource,             MainEntityType.datasource);
        entityMapping.put(EntityType.organization,           MainEntityType.organization);
        entityMapping.put(EntityType.project,                MainEntityType.project);
    }

    public static String getScheme(final String sourceType, final String targetType) {
        return String.format(schemeTemplate,
                entityMapping.get(EntityType.valueOf(sourceType)).name(),
                entityMapping.get(EntityType.valueOf(targetType)).name());
    }

    public static String getMainType(final String type) {
        return entityMapping.get(EntityType.valueOf(type)).name();
    }

    public static boolean isResult(String type) {
        return MainEntityType.result.name().equals(getMainType(type));
    }

    public static Predicate<String> instanceFilter = s -> instanceFieldFilter.contains(s);

    public static EntityRelEntity asRelatedEntity(EntityRelEntity e) {

        final DocumentContext j = JsonPath.parse(e.getSource().getOaf());
        final RelatedEntity re = new RelatedEntity().setId(j.read("$.id")).setType(e.getSource().getType());

        switch (EntityType.valueOf(e.getSource().getType())) {
            case publication:
            case dataset:
            case otherresearchproduct:
            case software:
                mapTitle(j, re);
                re.setDateofacceptance(j.read("$.dateofacceptance.value"));
                re.setPublisher(j.read("$.publisher.value"));

                JSONArray pids = j.read("$.pid");
                re.setPid(pids.stream()
                        .map(p -> asStructuredProperty((LinkedHashMap<String, Object>) p))
                        .collect(Collectors.toList()));

                re.setResulttype(asQualifier(j.read("$.resulttype")));

                JSONArray collfrom = j.read("$.collectedfrom");
                re.setCollectedfrom(collfrom.stream()
                        .map(c -> asKV((LinkedHashMap<String, Object>) c))
                        .collect(Collectors.toList()));

                // will throw exception when the instance is not found
                JSONArray instances = j.read("$.instance");
                re.setInstances(instances.stream()
                        .map(i -> {
                            final LinkedHashMap<String, Object> p = (LinkedHashMap<String, Object>) i;
                            final Field<String> license = new Field<String>();
                            license.setValue((String) ((LinkedHashMap<String, Object>) p.get("license")).get("value"));
                            final Instance instance = new Instance();
                            instance.setLicense(license);
                            instance.setAccessright(asQualifier((LinkedHashMap<String, String>) p.get("accessright")));
                            instance.setInstancetype(asQualifier((LinkedHashMap<String, String>) p.get("instancetype")));
                            instance.setHostedby(asKV((LinkedHashMap<String, Object>) p.get("hostedby")));
                            //TODO mapping of distributionlocation
                            instance.setCollectedfrom(asKV((LinkedHashMap<String, Object>) p.get("collectedfrom")));

                            Field<String> dateofacceptance = new Field<String>();
                            dateofacceptance.setValue((String) ((LinkedHashMap<String, Object>) p.get("dateofacceptance")).get("value"));
                            instance.setDateofacceptance(dateofacceptance);
                            return instance;
                        }).collect(Collectors.toList()));

                //TODO still to be mapped
                //re.setCodeRepositoryUrl(j.read("$.coderepositoryurl"));

                break;
            case datasource:
                re.setOfficialname(j.read("$.officialname.value"));
                re.setWebsiteurl(j.read("$.websiteurl.value"));
                re.setDatasourcetype(asQualifier(j.read("$.datasourcetype")));
                re.setOpenairecompatibility(asQualifier(j.read("$.openairecompatibility")));

                break;
            case organization:
                re.setLegalname(j.read("$.legalname.value"));
                re.setLegalshortname(j.read("$.legalshortname.value"));
                re.setCountry(asQualifier(j.read("$.country")));
                re.setWebsiteurl(j.read("$.websiteurl.value"));
                break;
            case project:
                re.setProjectTitle(j.read("$.title.value"));
                re.setCode(j.read("$.code.value"));
                re.setAcronym(j.read("$.acronym.value"));
                re.setContracttype(asQualifier(j.read("$.contracttype")));

                JSONArray f = j.read("$.fundingtree");
                if (!f.isEmpty()) {
                    re.setFundingtree(f.stream()
                            .map(s -> ((LinkedHashMap<String, String>) s).get("value"))
                            .collect(Collectors.toList()));
                }

                break;
        }
        return new EntityRelEntity().setSource(
                new TypedRow()
                        .setSourceId(e.getSource().getSourceId())
                        .setDeleted(e.getSource().getDeleted())
                        .setType(e.getSource().getType())
                        .setOaf(serialize(re)));
    }

    private static KeyValue asKV(LinkedHashMap<String, Object> j) {
        final KeyValue kv = new KeyValue();
        kv.setKey((String) j.get("key"));
        kv.setValue((String) j.get("value"));
        return kv;
    }

    private static void mapTitle(DocumentContext j, RelatedEntity re) {
        final JSONArray a = j.read("$.title");
        if (!a.isEmpty()) {
            final StructuredProperty sp = asStructuredProperty((LinkedHashMap<String, Object>) a.get(0));
            if (StringUtils.isNotBlank(sp.getValue())) {
                re.setTitle(sp);
            }
        }
    }

    private static StructuredProperty asStructuredProperty(LinkedHashMap<String, Object> j) {
        final StructuredProperty sp = new StructuredProperty();
        final String value = (String) j.get("value");
        if (StringUtils.isNotBlank(value)) {
            sp.setValue((String) j.get("value"));
            sp.setQualifier(asQualifier((LinkedHashMap<String, String>) j.get("qualifier")));
        }
        return sp;
    }

    public static Qualifier asQualifier(LinkedHashMap<String, String> j) {
        final Qualifier q = new Qualifier();

        final String classid = j.get("classid");
        if (StringUtils.isNotBlank(classid)) {
            q.setClassid(classid);
        }

        final String classname = j.get("classname");
        if (StringUtils.isNotBlank(classname)) {
            q.setClassname(classname);
        }

        final String schemeid = j.get("schemeid");
        if (StringUtils.isNotBlank(schemeid)) {
            q.setSchemeid(schemeid);
        }

        final String schemename = j.get("schemename");
        if (StringUtils.isNotBlank(schemename)) {
            q.setSchemename(schemename);
        }
        return q;
    }

    public static String serialize(final Object o) {
        try {
            return new ObjectMapper()
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                    .writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("unable to serialize: " + o.toString(), e);
        }
    }

    public static String removePrefix(final String s) {
        if (s.contains("|")) return substringAfter(s, "|");
        return s;
    }

    public static String getRelDescriptor(String relType, String subRelType, String relClass) {
        return relType + SEPARATOR + subRelType + SEPARATOR + relClass;
    }

}
