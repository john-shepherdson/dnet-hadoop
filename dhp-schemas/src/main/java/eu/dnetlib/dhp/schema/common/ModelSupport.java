
package eu.dnetlib.dhp.schema.common;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.codec.binary.Hex;
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
					.setRelation(ModelConstants.IS_PARTICIPANT)
					.setInverse(ModelConstants.HAS_PARTICIPANT)
					.setRelType(ModelConstants.PROJECT_ORGANIZATION)
					.setSubReltype(ModelConstants.PARTICIPATION));
		relationInverseMap
			.put(
				"projectOrganization_participation_hasParticipant", new RelationInverse()
					.setInverse(ModelConstants.IS_PARTICIPANT)
					.setRelation(ModelConstants.HAS_PARTICIPANT)
					.setRelType(ModelConstants.PROJECT_ORGANIZATION)
					.setSubReltype(ModelConstants.PARTICIPATION));
		relationInverseMap
			.put(
				"resultOrganization_affiliation_hasAuthorInstitution", new RelationInverse()
					.setRelation(ModelConstants.HAS_AUTHOR_INSTITUTION)
					.setInverse(ModelConstants.IS_AUTHOR_INSTITUTION_OF)
					.setRelType(ModelConstants.RESULT_ORGANIZATION)
					.setSubReltype(ModelConstants.AFFILIATION));
		relationInverseMap
			.put(
				"resultOrganization_affiliation_isAuthorInstitutionOf", new RelationInverse()
					.setInverse(ModelConstants.HAS_AUTHOR_INSTITUTION)
					.setRelation(ModelConstants.IS_AUTHOR_INSTITUTION_OF)
					.setRelType(ModelConstants.RESULT_ORGANIZATION)
					.setSubReltype(ModelConstants.AFFILIATION));
		relationInverseMap
			.put(
				"organizationOrganization_dedup_merges", new RelationInverse()
					.setRelation(ModelConstants.MERGES)
					.setInverse(ModelConstants.IS_MERGED_IN)
					.setRelType(ModelConstants.ORG_ORG_RELTYPE)
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"organizationOrganization_dedup_isMergedIn", new RelationInverse()
					.setInverse(ModelConstants.MERGES)
					.setRelation(ModelConstants.IS_MERGED_IN)
					.setRelType(ModelConstants.ORG_ORG_RELTYPE)
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"organizationOrganization_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse(ModelConstants.IS_SIMILAR_TO)
					.setRelation(ModelConstants.IS_SIMILAR_TO)
					.setRelType(ModelConstants.ORG_ORG_RELTYPE)
					.setSubReltype(ModelConstants.DEDUP));

		relationInverseMap
			.put(
				"resultProject_outcome_isProducedBy", new RelationInverse()
					.setRelation(ModelConstants.IS_PRODUCED_BY)
					.setInverse(ModelConstants.PRODUCES)
					.setRelType(ModelConstants.RESULT_PROJECT)
					.setSubReltype(ModelConstants.OUTCOME));
		relationInverseMap
			.put(
				"resultProject_outcome_produces", new RelationInverse()
					.setInverse(ModelConstants.IS_PRODUCED_BY)
					.setRelation(ModelConstants.PRODUCES)
					.setRelType(ModelConstants.RESULT_PROJECT)
					.setSubReltype(ModelConstants.OUTCOME));
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
					.setInverse(ModelConstants.IS_MERGED_IN)
					.setRelation(ModelConstants.MERGES)
					.setRelType("personPerson")
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"personPerson_dedup_isMergedIn", new RelationInverse()
					.setInverse(ModelConstants.MERGES)
					.setRelation(ModelConstants.IS_MERGED_IN)
					.setRelType("personPerson")
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"personPerson_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse(ModelConstants.IS_SIMILAR_TO)
					.setRelation(ModelConstants.IS_SIMILAR_TO)
					.setRelType("personPerson")
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"datasourceOrganization_provision_isProvidedBy", new RelationInverse()
					.setInverse(ModelConstants.PROVIDES)
					.setRelation(ModelConstants.IS_PROVIDED_BY)
					.setRelType(ModelConstants.DATASOURCE_ORGANIZATION)
					.setSubReltype(ModelConstants.PROVISION));
		relationInverseMap
			.put(
				"datasourceOrganization_provision_provides", new RelationInverse()
					.setInverse(ModelConstants.IS_PROVIDED_BY)
					.setRelation(ModelConstants.PROVIDES)
					.setRelType(ModelConstants.DATASOURCE_ORGANIZATION)
					.setSubReltype(ModelConstants.PROVISION));
		relationInverseMap
			.put(
				"resultResult_similarity_hasAmongTopNSimilarDocuments", new RelationInverse()
					.setInverse("isAmongTopNSimilarDocuments")
					.setRelation("hasAmongTopNSimilarDocuments")
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.SIMILARITY));
		relationInverseMap
			.put(
				"resultResult_similarity_isAmongTopNSimilarDocuments", new RelationInverse()
					.setInverse("hasAmongTopNSimilarDocuments")
					.setRelation("isAmongTopNSimilarDocuments")
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.SIMILARITY));
		relationInverseMap
			.put(
				"resultResult_relationship_isRelatedTo", new RelationInverse()
					.setInverse(ModelConstants.IS_RELATED_TO)
					.setRelation(ModelConstants.IS_RELATED_TO)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.RELATIONSHIP));
		relationInverseMap
			.put(
				"resultResult_supplement_isSupplementTo", new RelationInverse()
					.setInverse(ModelConstants.IS_SUPPLEMENTED_BY)
					.setRelation(ModelConstants.IS_SUPPLEMENT_TO)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.SUPPLEMENT));
		relationInverseMap
			.put(
				"resultResult_supplement_isSupplementedBy", new RelationInverse()
					.setInverse(ModelConstants.IS_SUPPLEMENT_TO)
					.setRelation(ModelConstants.IS_SUPPLEMENTED_BY)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.SUPPLEMENT));
		relationInverseMap
			.put(
				"resultResult_part_isPartOf", new RelationInverse()
					.setInverse(ModelConstants.HAS_PART)
					.setRelation(ModelConstants.IS_PART_OF)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.PART));
		relationInverseMap
			.put(
				"resultResult_part_hasPart", new RelationInverse()
					.setInverse(ModelConstants.IS_PART_OF)
					.setRelation(ModelConstants.HAS_PART)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.PART));
		relationInverseMap
			.put(
				"resultResult_dedup_merges", new RelationInverse()
					.setInverse(ModelConstants.IS_MERGED_IN)
					.setRelation(ModelConstants.MERGES)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"resultResult_dedup_isMergedIn", new RelationInverse()
					.setInverse(ModelConstants.MERGES)
					.setRelation(ModelConstants.IS_MERGED_IN)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.DEDUP));
		relationInverseMap
			.put(
				"resultResult_dedupSimilarity_isSimilarTo", new RelationInverse()
					.setInverse(ModelConstants.IS_SIMILAR_TO)
					.setRelation(ModelConstants.IS_SIMILAR_TO)
					.setRelType(ModelConstants.RESULT_RESULT)
					.setSubReltype(ModelConstants.DEDUP));

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
			AccessRight.class,
			OpenAccessRoute.class,
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

	public static String md5(final String s) {
		try {
			final MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(s.getBytes(StandardCharsets.UTF_8));
			return new String(Hex.encodeHex(md.digest()));
		} catch (final NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	public static String generateIdentifier(final String originalId, final String nsPrefix) {
		return String.format("%s::%s", nsPrefix, md5(originalId));
	}

	public static String oldest(String dateA, String dateB) throws ParseException {

		if (StringUtils.isBlank(dateA)) {
			return dateB;
		}
		if (StringUtils.isBlank(dateB)) {
			return dateA;
		}
		if (StringUtils.isNotBlank(dateA) && StringUtils.isNotBlank(dateB)) {

			final Date a = Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateA)));
			final Date b = Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateB)));

			return a.before(b) ? dateA : dateB;
		} else {
			return null;
		}
	}
}
