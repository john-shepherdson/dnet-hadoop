
package eu.dnetlib.dhp.schema.oaf.common;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import com.github.sisyphsu.dateparser.DateParserUtils;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.*;

/** Oaf model utility methods. */
public class ModelSupport {

	/** Defines the mapping between the actual entity type and the main entity type */
	private static final Map<EntityType, MainEntityType> entityMapping = Maps.newHashMap();

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
		set(relationInverseMap, PROJECT_ORGANIZATION, PARTICIPATION, IS_PARTICIPANT, HAS_PARTICIPANT);

		set(relationInverseMap, RESULT_ORGANIZATION, AFFILIATION, IS_AUTHOR_INSTITUTION_OF, HAS_AUTHOR_INSTITUTION);

		set(relationInverseMap, ORG_ORG_RELTYPE, DEDUP, IS_MERGED_IN, MERGES);
		set(relationInverseMap, ORG_ORG_RELTYPE, DEDUP, IS_SIMILAR_TO, IS_SIMILAR_TO);

		set(relationInverseMap, RESULT_PROJECT, OUTCOME, IS_PRODUCED_BY, PRODUCES);

		set(relationInverseMap, DATASOURCE_ORGANIZATION, PROVISION, IS_PROVIDED_BY, PROVIDES);

		set(relationInverseMap, RESULT_RESULT, SIMILARITY, IS_AMONG_TOP_N_SIMILAR_DOCS, HAS_AMONG_TOP_N_SIMILAR_DOCS);
		set(relationInverseMap, RESULT_RESULT, SUPPLEMENT, IS_SUPPLEMENT_TO, IS_SUPPLEMENTED_BY);
		set(relationInverseMap, RESULT_RESULT, PART, IS_PART_OF, HAS_PART);
		set(relationInverseMap, RESULT_RESULT, DEDUP, IS_MERGED_IN, MERGES);
		set(relationInverseMap, RESULT_RESULT, DEDUP, IS_SIMILAR_TO, IS_SIMILAR_TO);
		set(relationInverseMap, RESULT_RESULT, CITATION, IS_CITED_BY, CITES);

		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_IDENTICAL_TO, IS_IDENTICAL_TO);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_REFERENCED_BY, REFERENCES);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_CONTINUED_BY, CONTINUES);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_DOCUMENTED_BY, DOCUMENTS);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_DERIVED_FROM, IS_SOURCE_OF);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_RELATED_TO, IS_RELATED_TO);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_COMPILED_BY, COMPILES);

		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_DESCRIBED_BY, DESCRIBES);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_METADATA_FOR, IS_METADATA_OF);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, HAS_ASSOCIATION_WITH, HAS_ASSOCIATION_WITH);
		set(relationInverseMap, RESULT_RESULT, RELATIONSHIP, IS_REQUIRED_BY, REQUIRES);

		set(relationInverseMap, RESULT_RESULT, VERSION, IS_PREVIOUS_VERSION_OF, IS_NEW_VERSION_OF);
		set(relationInverseMap, RESULT_RESULT, VERSION, IS_VARIANT_FORM_OF, IS_ORIGINAL_FORM_OF);
		set(relationInverseMap, RESULT_RESULT, VERSION, IS_OBSOLETED_BY, OBSOLETES);
		set(relationInverseMap, RESULT_RESULT, VERSION, IS_VERSION_OF, HAS_VERSION);

		set(relationInverseMap, RESULT_RESULT, REVIEW, IS_REVIEWED_BY, REVIEWS);
	}

	private static void set(Map<String, RelationInverse> relationInverseMap, String relType, String subRelType,
		String relClass, String inverseRelClass) {
		relationInverseMap
			.put(
				rel(relType, subRelType, relClass), new RelationInverse()
					.setInverseRelClass(inverseRelClass)
					.setRelClass(relClass)
					.setRelType(relType)
					.setSubReltype(subRelType));
		if (!relClass.equals(inverseRelClass)) {
			relationInverseMap
				.put(
					rel(relType, subRelType, inverseRelClass), new RelationInverse()
						.setInverseRelClass(relClass)
						.setRelClass(inverseRelClass)
						.setRelType(relType)
						.setSubReltype(subRelType));
		}
	}

	/**
	 * Helper method: lookup relation inverse, given the direct relation encoding (case insensitive)
	 * @param encoding
	 * @return the relation inverse descriptor, throws @IllegalArgumentException when not found.
	 */
	public static RelationInverse findInverse(String encoding) {
		return ModelSupport.relationInverseMap
			.entrySet()
			.stream()
			.filter(r -> encoding.equalsIgnoreCase(r.getKey()))
			.findFirst()
			.map(r -> r.getValue())
			.orElseThrow(() -> new IllegalArgumentException("invalid relationship: " + encoding));
	}

	/**
	 * Helper method: fina a relation filtering by a relation name
	 * @param relationName
	 * @return
	 */
	public static RelationInverse findRelation(final String relationName) {
		return relationInverseMap
			.values()
			.stream()
			.filter(r -> relationName.equalsIgnoreCase(r.getRelClass()))
			.findFirst()
			.orElse(null);
	}

	/**
	 * Helper method: combines the relation attributes
	 * @param relType
	 * @param subRelType
	 * @param relClass
	 * @return
	 */
	public static String rel(String relType, String subRelType, String relClass) {
		return String.format("%s_%s_%s", relType, subRelType, relClass);
	}

	private static final String schemeTemplate = "dnet:%s_%s_relations";

	public static final String DATE_FORMAT = "yyyy-MM-dd";

	private ModelSupport() {
	}

	public static <E extends Entity> String getIdPrefix(Class<E> clazz) {
		return idPrefixMap.get(clazz);
	}

	public static <X extends Oaf, Y extends Oaf, Z extends Oaf> Boolean sameClass(X left, Y right, Class<Z> superClazz) {
		return isSubClass(left, superClazz) && isSubClass(right, superClazz);
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
	public static <T extends Entity> Class<T>[] getOafModelClasses() {
		return new Class[] {
			AccessRight.class,
			Author.class,
			AuthorPid.class,
			Context.class,
			Country.class,
			DataInfo.class,
			Dataset.class,
			Datasource.class,
			Entity.class,
			EntityDataInfo.class,
			EoscIfGuidelines.class,
			ExternalReference.class,
			ExtraInfo.class,
			GeoLocation.class,
			H2020Classification.class,
			H2020Programme.class,
			Instance.class,
			Journal.class,
			KeyValue.class,
			License.class,
			Measure.class,
			OAIProvenance.class,
			OpenAccessRoute.class,
			Organization.class,
			OriginDescription.class,
			OtherResearchProduct.class,
			Project.class,
			Provenance.class,
			Publication.class,
			Publisher.class,
			Qualifier.class,
			Relation.class,
			Result.class,
			Software.class,
			StructuredProperty.class,
			Subject.class
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

	public static String tableIdentifier(String dbName, String tableName) {

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
		return ((Entity) t).getId();
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

			final Date a = DateParserUtils.parseDate(dateA);
			final Date b = DateParserUtils.parseDate(dateB);

			if (Objects.nonNull(a) && Objects.nonNull(b)) {
				return a.before(b) ? dateA : dateB;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

}
