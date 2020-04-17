package eu.dnetlib.dhp.schema.common;

import com.google.common.collect.Maps;
import eu.dnetlib.dhp.schema.oaf.*;

import java.util.Map;

/**
 * Oaf model utility methods.
 */
public class ModelSupport {

    /**
     * Defines the mapping between the actual entity type and the main entity type
     */
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

    /**
     * Defines the mapping between the actual entity types and the relative classes implementing them
     */
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

    public final static Map<String, Class> oafTypes = Maps.newHashMap();

    static {
        oafTypes.put("datasource", Datasource.class);
        oafTypes.put("organization", Organization.class);
        oafTypes.put("project", Project.class);
        oafTypes.put("dataset", Dataset.class);
        oafTypes.put("otherresearchproduct", OtherResearchProduct.class);
        oafTypes.put("software", Software.class);
        oafTypes.put("publication", Publication.class);
        oafTypes.put("relation", Relation.class);
    }

    private static final String schemeTemplate = "dnet:%s_%s_relations";

    private ModelSupport() {
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazzObject   Subclass object instance
     * @param superClazzObject Superclass object instance
     * @param <X>              Subclass type
     * @param <Y>              Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(X subClazzObject, Y superClazzObject) {
        return isSubClass(subClazzObject.getClass(), superClazzObject.getClass());
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazzObject Subclass object instance
     * @param superClazz     Superclass class
     * @param <X>            Subclass type
     * @param <Y>            Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(X subClazzObject, Class<Y> superClazz) {
        return isSubClass(subClazzObject.getClass(), superClazz);
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazz   Subclass class
     * @param superClazz Superclass class
     * @param <X>        Subclass type
     * @param <Y>        Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(Class<X> subClazz, Class<Y> superClazz) {
        return superClazz.isAssignableFrom(subClazz);
    }

    /**
     * Lists all the OAF model classes
     *
     * @param <T>
     * @return
     */
    public static <T extends Oaf> Class<T>[] getOafModelClasses() {
        return new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                Dataset.class,
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
                Result.class,
                Software.class,
                StructuredProperty.class
        };
    }

    public static String getMainType(final EntityType type) {
        return entityMapping.get(type).name();
    }

    public static boolean isResult(EntityType type) {
        return MainEntityType.result.name().equals(getMainType(type));
    }

    public static String getScheme(final String sourceType, final String targetType) {
        return String.format(schemeTemplate,
                entityMapping.get(EntityType.valueOf(sourceType)).name(),
                entityMapping.get(EntityType.valueOf(targetType)).name());
    }

}