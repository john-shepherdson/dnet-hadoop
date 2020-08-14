
package eu.dnetlib.dhp.schema.common;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.*;

/** Oaf model utility methods. */
public class ModelSupport {

	/** Defines the mapping between the actual entity type and the main entity type */
	private static Map<EntityType, MainEntityType> entityMapping = Maps.newHashMap();

	static {
		entityMapping.put(EntityType.publication, MainEntityType.result);
		entityMapping.put(EntityType.dataset, MainEntityType.result);
		entityMapping.put(EntityType.otherresearchproduct, MainEntityType.result);
		entityMapping.put(EntityType.software, MainEntityType.result);
		entityMapping.put(EntityType.datasource, MainEntityType.datasource);
		entityMapping.put(EntityType.organization, MainEntityType.organization);
		entityMapping.put(EntityType.project, MainEntityType.project);
	}

	/**
	 * Defines the mapping between the actual entity types and the relative classes implementing them
	 */
	public static final Map<EntityType, Class> entityTypes = Maps.newHashMap();

	static {
		entityTypes.put(EntityType.datasource, Datasource.class);
		entityTypes.put(EntityType.organization, Organization.class);
		entityTypes.put(EntityType.project, Project.class);
		entityTypes.put(EntityType.dataset, Dataset.class);
		entityTypes.put(EntityType.otherresearchproduct, OtherResearchProduct.class);
		entityTypes.put(EntityType.software, Software.class);
		entityTypes.put(EntityType.publication, Publication.class);
	}

	public static final Map<String, Class> oafTypes = Maps.newHashMap();

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

	public static final Map<Class, String> idPrefixMap = Maps.newHashMap();

	static {
		idPrefixMap.put(Datasource.class, "10");
		idPrefixMap.put(Organization.class, "20");
		idPrefixMap.put(Project.class, "40");
		idPrefixMap.put(Dataset.class, "50");
		idPrefixMap.put(OtherResearchProduct.class, "50");
		idPrefixMap.put(Software.class, "50");
		idPrefixMap.put(Publication.class, "50");
	}

	public static final Map<String, String> entityIdPrefix = Maps.newHashMap();

	static {
		entityIdPrefix.put("datasource", "10");
		entityIdPrefix.put("organization", "20");
		entityIdPrefix.put("project", "40");
		entityIdPrefix.put("result", "50");
	}

	public static final Map<String, String> idPrefixEntity = Maps.newHashMap();

	static {
		idPrefixEntity.put("10", "datasource");
		idPrefixEntity.put("20", "organization");
		idPrefixEntity.put("40", "project");
		idPrefixEntity.put("50", "result");
	}

	public static final Map<String, RelationInverse> relationInverseMap = Maps.newHashMap();

	static {
		relationInverseMap
			.put(
				"personResult_authorship_isAuthorOf", new RelationInverse()
					.setRelation("isAuthorOf")
					.setInverse("hasAuthor")
					.setRelType("personResult")
					.setSubReltype("authorship"));
		relationInverseMap
			.put(
				"personResult_authorship_hasAuthor", new RelationInverse()
					.setInverse("isAuthorOf")
					.setRelation("hasAuthor")
					.setRelType("personResult")
					.setSubReltype("authorship"));
		relationInverseMap
			.put(
				"projectOrganization_participation_isParticipant", new RelationInverse()
					.setRelation("isParticipant")
					.setInverse("hasParticipant")
					.setRelType("projectOrganization")
					.setSubReltype("participation"));
		relationInverseMap
			.put(
				"projectOrganization_participation_hasParticipant", new RelationInverse()
					.setInverse("isParticipant")
					.setRelation("hasParticipant")
					.setRelType("projectOrganization")
					.setSubReltype("participation"));
		relationInverseMap
			.put(
				"resultOrganization_affiliation_hasAuthorInstitution", new RelationInverse()
					.setRelation("hasAuthorInstitution")
					.setInverse("isAuthorInstitutionOf")
					.setRelType("resultOrganization")
					.setSubReltype("affiliation"));
		relationInverseMap
			.put(
				"resultOrganization_affiliation_isAuthorInstitutionOf", new RelationInverse()
					.setInverse("hasAuthorInstitution")
					.setRelation("isAuthorInstitutionOf")
					.setRelType("resultOrganization")
					.setSubReltype("affiliation"));
		relationInverseMap
			.put(
				"organizationOrganization_dedup_merges", new RelationInverse()
					.setRelation("merges")
					.setInverse("isMergedIn")
					.setRelType("organizationOrganization")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"organizationOrganization_dedup_isMergedIn", new RelationInverse()
					.setInverse("merges")
					.setRelation("isMergedIn")
					.setRelType("organizationOrganization")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"organizationOrganization_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse("isSimilarTo")
					.setRelation("isSimilarTo")
					.setRelType("organizationOrganization")
					.setSubReltype("dedupSimilarity"));

		relationInverseMap
			.put(
				"resultProject_outcome_isProducedBy", new RelationInverse()
					.setRelation("isProducedBy")
					.setInverse("produces")
					.setRelType("resultProject")
					.setSubReltype("outcome"));
		relationInverseMap
			.put(
				"resultProject_outcome_produces", new RelationInverse()
					.setInverse("isProducedBy")
					.setRelation("produces")
					.setRelType("resultProject")
					.setSubReltype("outcome"));
		relationInverseMap
			.put(
				"projectPerson_contactPerson_isContact", new RelationInverse()
					.setRelation("isContact")
					.setInverse("hasContact")
					.setRelType("projectPerson")
					.setSubReltype("contactPerson"));
		relationInverseMap
			.put(
				"projectPerson_contactPerson_hasContact", new RelationInverse()
					.setInverse("isContact")
					.setRelation("hasContact")
					.setRelType("personPerson")
					.setSubReltype("coAuthorship"));
		relationInverseMap
			.put(
				"personPerson_coAuthorship_isCoauthorOf", new RelationInverse()
					.setInverse("isCoAuthorOf")
					.setRelation("isCoAuthorOf")
					.setRelType("personPerson")
					.setSubReltype("coAuthorship"));
		relationInverseMap
			.put(
				"personPerson_dedup_merges", new RelationInverse()
					.setInverse("isMergedIn")
					.setRelation("merges")
					.setRelType("personPerson")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"personPerson_dedup_isMergedIn", new RelationInverse()
					.setInverse("merges")
					.setRelation("isMergedIn")
					.setRelType("personPerson")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"personPerson_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse("isSimilarTo")
					.setRelation("isSimilarTo")
					.setRelType("personPerson")
					.setSubReltype("dedupSimilarity"));
		relationInverseMap
			.put(
				"datasourceOrganization_provision_isProvidedBy", new RelationInverse()
					.setInverse("provides")
					.setRelation("isProvidedBy")
					.setRelType("datasourceOrganization")
					.setSubReltype("provision"));
		relationInverseMap
			.put(
				"datasourceOrganization_provision_provides", new RelationInverse()
					.setInverse("isProvidedBy")
					.setRelation("provides")
					.setRelType("datasourceOrganization")
					.setSubReltype("provision"));
		relationInverseMap
			.put(
				"resultResult_similarity_hasAmongTopNSimilarDocuments", new RelationInverse()
					.setInverse("isAmongTopNSimilarDocuments")
					.setRelation("hasAmongTopNSimilarDocuments")
					.setRelType("resultResult")
					.setSubReltype("similarity"));
		relationInverseMap
			.put(
				"resultResult_similarity_isAmongTopNSimilarDocuments", new RelationInverse()
					.setInverse("hasAmongTopNSimilarDocuments")
					.setRelation("isAmongTopNSimilarDocuments")
					.setRelType("resultResult")
					.setSubReltype("similarity"));
		relationInverseMap
			.put(
				"resultResult_relationship_isRelatedTo", new RelationInverse()
					.setInverse("isRelatedTo")
					.setRelation("isRelatedTo")
					.setRelType("resultResult")
					.setSubReltype("relationship"));
		relationInverseMap
			.put(
				"resultResult_similarity_isAmongTopNSimilarDocuments", new RelationInverse()
					.setInverse("hasAmongTopNSimilarDocuments")
					.setRelation("isAmongTopNSimilarDocuments")
					.setRelType("resultResult")
					.setSubReltype("similarity"));
		relationInverseMap
			.put(
				"resultResult_supplement_isSupplementTo", new RelationInverse()
					.setInverse("isSupplementedBy")
					.setRelation("isSupplementTo")
					.setRelType("resultResult")
					.setSubReltype("supplement"));
		relationInverseMap
			.put(
				"resultResult_supplement_isSupplementedBy", new RelationInverse()
					.setInverse("isSupplementTo")
					.setRelation("isSupplementedBy")
					.setRelType("resultResult")
					.setSubReltype("supplement"));
		relationInverseMap
			.put(
				"resultResult_part_isPartOf", new RelationInverse()
					.setInverse("hasPart")
					.setRelation("isPartOf")
					.setRelType("resultResult")
					.setSubReltype("part"));
		relationInverseMap
			.put(
				"resultResult_part_hasPart", new RelationInverse()
					.setInverse("isPartOf")
					.setRelation("hasPart")
					.setRelType("resultResult")
					.setSubReltype("part"));
		relationInverseMap
			.put(
				"resultResult_dedup_merges", new RelationInverse()
					.setInverse("isMergedIn")
					.setRelation("merges")
					.setRelType("resultResult")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"resultResult_dedup_isMergedIn", new RelationInverse()
					.setInverse("merges")
					.setRelation("isMergedIn")
					.setRelType("resultResult")
					.setSubReltype("dedup"));
		relationInverseMap
			.put(
				"resultResult_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse("isSimilarTo")
					.setRelation("isSimilarTo")
					.setRelType("resultResult")
					.setSubReltype("dedupSimilarity"));

	}

	private static final String schemeTemplate = "dnet:%s_%s_relations";

	private ModelSupport() {
	}

	public static <E extends OafEntity> String getIdPrefix(Class<E> clazz) {
		return idPrefixMap.get(clazz);
	}

	/**
	 * Checks subclass-superclass relationship.
	 *
	 * @param subClazzObject Subclass object instance
	 * @param superClazzObject Superclass object instance
	 * @param <X> Subclass type
	 * @param <Y> Superclass type
	 * @return True if X is a subclass of Y
	 */
	public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(
		X subClazzObject, Y superClazzObject) {
		return isSubClass(subClazzObject.getClass(), superClazzObject.getClass());
	}

	/**
	 * Checks subclass-superclass relationship.
	 *
	 * @param subClazzObject Subclass object instance
	 * @param superClazz Superclass class
	 * @param <X> Subclass type
	 * @param <Y> Superclass type
	 * @return True if X is a subclass of Y
	 */
	public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(
		X subClazzObject, Class<Y> superClazz) {
		return isSubClass(subClazzObject.getClass(), superClazz);
	}

	/**
	 * Checks subclass-superclass relationship.
	 *
	 * @param subClazz Subclass class
	 * @param superClazz Superclass class
	 * @param <X> Subclass type
	 * @param <Y> Superclass type
	 * @return True if X is a subclass of Y
	 */
	public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(
		Class<X> subClazz, Class<Y> superClazz) {
		return superClazz.isAssignableFrom(subClazz);
	}

	/**
	 * Lists all the OAF model classes
	 *
	 * @param <T>
	 * @return
	 */
	public static <T extends Oaf> Class<T>[] getOafModelClasses() {
		return new Class[] {
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
		return String
			.format(
				schemeTemplate,
				entityMapping.get(EntityType.valueOf(sourceType)).name(),
				entityMapping.get(EntityType.valueOf(targetType)).name());
	}

	public static <T extends Oaf> String tableIdentifier(String dbName, String tableName) {

		checkArgument(StringUtils.isNotBlank(dbName), "DB name cannot be empty");
		checkArgument(StringUtils.isNotBlank(tableName), "table name cannot be empty");

		return String.format("%s.%s", dbName, tableName);
	}

	public static <T extends Oaf> String tableIdentifier(String dbName, Class<T> clazz) {

		checkArgument(Objects.nonNull(clazz), "clazz is needed to derive the table name, thus cannot be null");

		return tableIdentifier(dbName, clazz.getSimpleName().toLowerCase());
	}

	public static <T extends Oaf> Function<T, String> idFn() {
		return x -> {
			if (isSubClass(x, Relation.class)) {
				return idFnForRelation(x);
			}
			return idFnForOafEntity(x);
		};
	}

	private static <T extends Oaf> String idFnForRelation(T t) {
		Relation r = (Relation) t;
		return Optional
			.ofNullable(r.getSource())
			.map(
				source -> Optional
					.ofNullable(r.getTarget())
					.map(
						target -> Optional
							.ofNullable(r.getRelType())
							.map(
								relType -> Optional
									.ofNullable(r.getSubRelType())
									.map(
										subRelType -> Optional
											.ofNullable(r.getRelClass())
											.map(
												relClass -> String
													.join(
														source,
														target,
														relType,
														subRelType,
														relClass))
											.orElse(
												String
													.join(
														source,
														target,
														relType,
														subRelType)))
									.orElse(String.join(source, target, relType)))
							.orElse(String.join(source, target)))
					.orElse(source))
			.orElse(null);
	}

	private static <T extends Oaf> String idFnForOafEntity(T t) {
		return ((OafEntity) t).getId();
	}
}
