package eu.dnetlib.dhp.oa.provision.utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
import eu.dnetlib.dhp.oa.provision.model.SortableRelation;
import eu.dnetlib.dhp.schema.oaf.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.substringAfter;

public class GraphMappingUtils {

    public static final String SEPARATOR = "_";

    public final static Map<EntityType, Class> entityTypes = Maps.newHashMap();

    static {
        entityTypes.put(EntityType.datasource, Datasource.class);
        entityTypes.put(EntityType.organization, Organization.class);
        entityTypes.put(EntityType.project, Project.class);
        entityTypes.put(EntityType.dataset, Dataset.class);
        entityTypes.put(EntityType.otherresearchproduct, OtherResearchProduct.class);
        entityTypes.put(EntityType.software, Software.class);
        entityTypes.put(EntityType.publication, Publication.class);
    }

    public enum EntityType {
        publication, dataset, otherresearchproduct, software, datasource, organization, project;

        public static <T extends OafEntity> EntityType fromClass(Class<T> clazz) {
            switch (clazz.getName()) {
                case "eu.dnetlib.dhp.schema.oaf.Publication"            : return publication;
                case "eu.dnetlib.dhp.schema.oaf.Dataset"                : return dataset;
                case "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct"   : return otherresearchproduct;
                case "eu.dnetlib.dhp.schema.oaf.Software"               : return software;
                case "eu.dnetlib.dhp.schema.oaf.Datasource"             : return datasource;
                case "eu.dnetlib.dhp.schema.oaf.Organization"           : return organization;
                case "eu.dnetlib.dhp.schema.oaf.Project"                : return project;
                default: throw new IllegalArgumentException("Unknown OafEntity class: " + clazz.getName());
            }
        }
    }

    public enum MainEntityType {
        result, datasource, organization, project
    }

    public static Set<String> authorPidTypes = Sets.newHashSet("orcid", "magidentifier");

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

    public static Class[] getKryoClasses() {
        return new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                eu.dnetlib.dhp.schema.oaf.Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                SortableRelation.class, //SUPPORT
                Result.class,
                Software.class,
                StructuredProperty.class
        };
    }

    public static String getScheme(final String sourceType, final String targetType) {
        return String.format(schemeTemplate,
                entityMapping.get(EntityType.valueOf(sourceType)).name(),
                entityMapping.get(EntityType.valueOf(targetType)).name());
    }

    public static String getMainType(final EntityType type) {
        return entityMapping.get(type).name();
    }

    public static boolean isResult(EntityType type) {
        return MainEntityType.result.name().equals(getMainType(type));
    }

    public static <E extends OafEntity> RelatedEntity asRelatedEntity(E entity, Class<E> clazz) {

        final RelatedEntity re = new RelatedEntity();
        re.setId(entity.getId());
        re.setType(EntityType.fromClass(clazz).name());

        re.setPid(entity.getPid());
        re.setCollectedfrom(entity.getCollectedfrom());

        switch (GraphMappingUtils.EntityType.fromClass(clazz)) {
            case publication:
            case dataset:
            case otherresearchproduct:
            case software:

                Result result = (Result) entity;

                if (result.getTitle() == null && !result.getTitle().isEmpty()) {
                    re.setTitle(result.getTitle().stream().findFirst().get());
                }

                re.setDateofacceptance(getValue(result.getDateofacceptance()));
                re.setPublisher(getValue(result.getPublisher()));
                re.setResulttype(result.getResulttype());
                re.setInstances(result.getInstance());

                //TODO still to be mapped
                //re.setCodeRepositoryUrl(j.read("$.coderepositoryurl"));

                break;
            case datasource:
                Datasource d = (Datasource) entity;

                re.setOfficialname(getValue(d.getOfficialname()));
                re.setWebsiteurl(getValue(d.getWebsiteurl()));
                re.setDatasourcetype(d.getDatasourcetype());
                re.setOpenairecompatibility(d.getOpenairecompatibility());

                break;
            case organization:
                Organization o = (Organization) entity;

                re.setLegalname(getValue(o.getLegalname()));
                re.setLegalshortname(getValue(o.getLegalshortname()));
                re.setCountry(o.getCountry());
                re.setWebsiteurl(getValue(o.getWebsiteurl()));
                break;
            case project:
                Project p = (Project) entity;

                re.setProjectTitle(getValue(p.getTitle()));
                re.setCode(getValue(p.getCode()));
                re.setAcronym(getValue(p.getAcronym()));
                re.setContracttype(p.getContracttype());

                List<Field<String>> f = p.getFundingtree();
                if (!f.isEmpty()) {
                    re.setFundingtree(f.stream()
                            .map(s -> s.getValue())
                            .collect(Collectors.toList()));
                }
                break;
        }
        return re;
    }

    private static String getValue(Field<String> field) {
        return getFieldValueWithDefault(field, "");
    }

    private static <T> T getFieldValueWithDefault(Field<T> f, T defaultValue) {
        return Optional.ofNullable(f)
                .filter(Objects::nonNull)
                .map(x -> x.getValue())
                .orElse(defaultValue);
    }

    public static String removePrefix(final String s) {
        if (s.contains("|")) return substringAfter(s, "|");
        return s;
    }

    public static String getRelDescriptor(String relType, String subRelType, String relClass) {
        return relType + SEPARATOR + subRelType + SEPARATOR + relClass;
    }

}
