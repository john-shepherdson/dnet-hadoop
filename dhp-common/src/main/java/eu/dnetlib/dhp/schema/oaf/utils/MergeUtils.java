
package eu.dnetlib.dhp.schema.oaf.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.github.sisyphsu.dateparser.DateParserUtils;
import com.google.common.base.Joiner;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.common.AccessRightComparator;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class MergeUtils {

	public static <T extends Oaf> T mergeById(String s, Iterator<T> oafEntityIterator) {
		return mergeGroup(s, oafEntityIterator, true);
	}

	public static <T extends Oaf> T mergeGroup(String s, Iterator<T> oafEntityIterator) {
		return mergeGroup(s, oafEntityIterator, false);
	}

	public static <T extends Oaf> T mergeGroup(String s, Iterator<T> oafEntityIterator,
		boolean checkDelegateAuthority) {

		ArrayList<T> sortedEntities = new ArrayList<>();
		oafEntityIterator.forEachRemaining(sortedEntities::add);
		sortedEntities.sort(MergeEntitiesComparator.INSTANCE.reversed());

		Iterator<T> it = sortedEntities.iterator();
		T merged = it.next();

		while (it.hasNext()) {
			merged = checkedMerge(merged, it.next(), checkDelegateAuthority);
		}

		return merged;
	}

	public static <T extends Oaf> T checkedMerge(final T left, final T right, boolean checkDelegateAuthority) {
		return (T) merge(left, right, checkDelegateAuthority);
	}

	public static <T extends Result, E extends Result> Result mergeResult(final T left, final E right) {
		return (Result) merge(left, right, false);
	}

	public static Oaf merge(final Oaf left, final Oaf right) {
		return merge(left, right, false);
	}

	static Oaf merge(final Oaf left, final Oaf right, boolean checkDelegatedAuthority) {
		if (sameClass(left, right, OafEntity.class)) {
			return mergeEntities(left, right, checkDelegatedAuthority);
		} else if (sameClass(left, right, Relation.class)) {
			return mergeRelation((Relation) left, (Relation) right);
		} else {
			throw new RuntimeException(
				String
					.format(
						"MERGE_FROM_AND_GET incompatible types: %s, %s",
						left.getClass().getCanonicalName(), right.getClass().getCanonicalName()));
		}
	}

	private static <T extends Oaf> boolean sameClass(Object left, Object right, Class<T> cls) {
		return cls.isAssignableFrom(left.getClass()) && cls.isAssignableFrom(right.getClass());
	}

	private static Oaf mergeEntities(Oaf left, Oaf right, boolean checkDelegatedAuthority) {

		if (sameClass(left, right, Result.class)) {
			if (checkDelegatedAuthority) {
				return mergeResultsOfDifferentTypes((Result) left, (Result) right);
			}

			if (sameClass(left, right, Publication.class)) {
				return mergePublication((Publication) left, (Publication) right);
			}
			if (sameClass(left, right, Dataset.class)) {
				return mergeDataset((Dataset) left, (Dataset) right);
			}
			if (sameClass(left, right, OtherResearchProduct.class)) {
				return mergeORP((OtherResearchProduct) left, (OtherResearchProduct) right);
			}
			if (sameClass(left, right, Software.class)) {
				return mergeSoftware((Software) left, (Software) right);
			}

			return mergeResultFields((Result) left, (Result) right);
		} else if (sameClass(left, right, Datasource.class)) {
			// TODO
			final int trust = compareTrust(left, right);
			return mergeOafEntityFields((Datasource) left, (Datasource) right, trust);
		} else if (sameClass(left, right, Organization.class)) {
			return mergeOrganization((Organization) left, (Organization) right);
		} else if (sameClass(left, right, Project.class)) {
			return mergeProject((Project) left, (Project) right);
		} else {
			throw new RuntimeException(
				String
					.format(
						"MERGE_FROM_AND_GET incompatible types: %s, %s",
						left.getClass().getCanonicalName(), right.getClass().getCanonicalName()));
		}
	}

	/**
	 * This method is used in the global result grouping phase. It checks if one of the two is from a delegated authority
	 * https://graph.openaire.eu/docs/data-model/pids-and-identifiers#delegated-authorities and in that case it prefers
	 * such version.
	 * <p>
	 * Otherwise, it considers a resulttype priority order implemented in {@link MergeEntitiesComparator}
	 * and proceeds with the canonical property merging.
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	private static <T extends Result> T mergeResultsOfDifferentTypes(T left, T right) {

		final boolean leftFromDelegatedAuthority = isFromDelegatedAuthority(left);
		final boolean rightFromDelegatedAuthority = isFromDelegatedAuthority(right);

		if (leftFromDelegatedAuthority && !rightFromDelegatedAuthority) {
			return left;
		}
		if (!leftFromDelegatedAuthority && rightFromDelegatedAuthority) {
			return right;
		}

		// TODO: raise trust to have preferred fields from one or the other??
		if (MergeEntitiesComparator.INSTANCE.compare(left, right) > 0) {
			return mergeResultFields(left, right);
		} else {
			return mergeResultFields(right, left);
		}
	}

	private static DataInfo chooseDataInfo(DataInfo left, DataInfo right, int trust) {
		if (trust > 0) {
			return left;
		} else if (trust == 0) {
			if (left == null || (left.getInvisible() != null && left.getInvisible().equals(Boolean.TRUE))) {
				return right;
			} else {
				return left;
			}
		} else {
			return right;
		}
	}

	private static String chooseString(String left, String right, int trust) {
		if (trust > 0) {
			return left;
		} else if (trust == 0) {
			return StringUtils.isNotBlank(left) ? left : right;
		} else {
			return right;
		}
	}

	private static <T> T chooseReference(T left, T right, int trust) {
		if (trust > 0) {
			return left;
		} else if (trust == 0) {
			return left != null ? left : right;
		} else {
			return right;
		}
	}

	private static Long max(Long left, Long right) {
		if (left == null)
			return right;
		if (right == null)
			return left;

		return Math.max(left, right);
	}

	// trust ??
	private static Boolean booleanOR(Boolean a, Boolean b) {
		if (a == null) {
			return b;
		} else if (b == null) {
			return a;
		}

		return a || b;
	}

	private static <T, K> List<T> mergeLists(final List<T> left, final List<T> right, int trust,
		Function<T, K> keyExtractor, BinaryOperator<T> merger) {
		if (left == null || left.isEmpty()) {
			return right != null ? right : new ArrayList<>();
		} else if (right == null || right.isEmpty()) {
			return left;
		}

		List<T> h = trust >= 0 ? left : right;
		List<T> l = trust >= 0 ? right : left;

		return new ArrayList<>(Stream
			.concat(h.stream(), l.stream())
			.filter(Objects::nonNull)
			.distinct()
			.collect(Collectors.toMap(keyExtractor, v -> v, merger, LinkedHashMap::new))
			.values());
	}

	private static <T, K> List<T> unionDistinctLists(final List<T> left, final List<T> right, int trust) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}

		List<T> h = trust >= 0 ? left : right;
		List<T> l = trust >= 0 ? right : left;

		return Stream
			.concat(h.stream(), l.stream())
			.filter(Objects::nonNull)
			.distinct()
			.collect(Collectors.toList());
	}

	private static List<String> unionDistinctListOfString(final List<String> l, final List<String> r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
		}

		return Stream
			.concat(l.stream(), r.stream())
			.filter(StringUtils::isNotBlank)
			.distinct()
			.collect(Collectors.toList());
	}

	// TODO review
	private static List<KeyValue> mergeByKey(List<KeyValue> left, List<KeyValue> right, int trust) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}

		if (trust < 0) {
			List<KeyValue> s = left;
			left = right;
			right = s;
		}

		HashMap<String, KeyValue> values = new HashMap<>();

		Optional.ofNullable(left).ifPresent(l -> l.forEach(kv -> values.put(kv.getKey(), kv)));
		Optional.ofNullable(right).ifPresent(r -> r.forEach(kv -> values.putIfAbsent(kv.getKey(), kv)));

		return new ArrayList<>(values.values());
	}

	// TODO review
	private static List<KeyValue> appendKey(List<KeyValue> left, List<KeyValue> right, int trust) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}

		if (trust < 0) {
			List<KeyValue> s = left;
			left = right;
			right = s;
		}

		List<KeyValue> collect = unionDistinctLists(
			left.stream().map(HashableKeyValue::newInstance).collect(Collectors.toList()),
			right.stream().map(HashableKeyValue::newInstance).collect(Collectors.toList()), trust)
				.stream()
				.map(HashableKeyValue::toKeyValue)
				.collect(Collectors.toList());
		return collect;

	}

	private static List<StructuredProperty> unionTitle(List<StructuredProperty> left, List<StructuredProperty> right,
		int trust) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		}

		List<StructuredProperty> h = trust >= 0 ? left : right;
		List<StructuredProperty> l = trust >= 0 ? right : left;

		return Stream
			.concat(h.stream(), l.stream())
			.filter(Objects::isNull)
			.distinct()
			.collect(Collectors.toList());
	}

	/**
	 * Internal utility that merges the common OafEntity fields
	 *
	 * @param merged
	 * @param enrich
	 * @param <T>
	 * @return
	 */
	private static <T extends Oaf> T mergeOafFields(T merged, T enrich, int trust) {

		merged.setCollectedfrom(mergeByKey(merged.getCollectedfrom(), enrich.getCollectedfrom(), trust));
		merged.setDataInfo(chooseDataInfo(merged.getDataInfo(), enrich.getDataInfo(), trust));
		merged.setLastupdatetimestamp(max(merged.getLastupdatetimestamp(), enrich.getLastupdatetimestamp()));

		return merged;
	}

	/**
	 * Internal utility that merges the common OafEntity fields
	 *
	 * @param original
	 * @param enrich
	 * @param <T>
	 * @return
	 */
	private static <T extends OafEntity> T mergeOafEntityFields(T original, T enrich, int trust) {
		final T merged = mergeOafFields(original, enrich, trust);

		merged.setOriginalId(unionDistinctListOfString(merged.getOriginalId(), enrich.getOriginalId()));
		merged.setPid(mergeLists(merged.getPid(), enrich.getPid(), trust, MergeUtils::spKeyExtractor, (p1, p2) -> p1));
		merged.setDateofcollection(LocalDateTime.now().toString());
		merged
			.setDateoftransformation(
				chooseString(merged.getDateoftransformation(), enrich.getDateoftransformation(), trust));
		merged.setExtraInfo(unionDistinctLists(merged.getExtraInfo(), enrich.getExtraInfo(), trust));
		// When merging records OAI provenance becomes null
		merged.setOaiprovenance(null);
		merged.setMeasures(unionDistinctLists(merged.getMeasures(), enrich.getMeasures(), trust));

		return merged;
	}

	public static <T extends Relation> T mergeRelation(T original, T enrich) {
		int trust = compareTrust(original, enrich);
		T merge = mergeOafFields(original, enrich, trust);

		checkArgument(Objects.equals(merge.getSource(), enrich.getSource()), "source ids must be equal");
		checkArgument(Objects.equals(merge.getTarget(), enrich.getTarget()), "target ids must be equal");
		checkArgument(Objects.equals(merge.getRelType(), enrich.getRelType()), "relType(s) must be equal");
		checkArgument(
			Objects.equals(merge.getSubRelType(), enrich.getSubRelType()), "subRelType(s) must be equal");
		checkArgument(Objects.equals(merge.getRelClass(), enrich.getRelClass()), "relClass(es) must be equal");

		// merge.setProvenance(mergeLists(merge.getProvenance(), enrich.getProvenance()));

		// TODO: trust ??
		merge.setValidated(booleanOR(merge.getValidated(), enrich.getValidated()));
		try {
			merge.setValidationDate(ModelSupport.oldest(merge.getValidationDate(), enrich.getValidationDate()));
		} catch (ParseException e) {
			throw new IllegalArgumentException(String
				.format(
					"invalid validation date format in relation [s:%s, t:%s]: %s", merge.getSource(),
					merge.getTarget(),
					merge.getValidationDate()));
		}

		// TODO keyvalue merge
		merge.setProperties(appendKey(merge.getProperties(), enrich.getProperties(), trust));

		return merge;
	}

	private static <T extends Result> T mergeResultFields(T original, T enrich) {
		final int trust = compareTrust(original, enrich);
		T merge = mergeOafEntityFields(original, enrich, trust);

		if (merge.getProcessingchargeamount() == null
			|| StringUtils.isBlank(merge.getProcessingchargeamount().getValue())) {
			merge.setProcessingchargeamount(enrich.getProcessingchargeamount());
			merge.setProcessingchargecurrency(enrich.getProcessingchargecurrency());
		}

		merge.setAuthor(mergeAuthors(merge.getAuthor(), enrich.getAuthor(), trust));

		// keep merge value if present
		if (merge.getResulttype() == null) {
			merge.setResulttype(enrich.getResulttype());
			merge.setMetaResourceType(enrich.getMetaResourceType());
		}

		// should be an instance attribute, get the first non-null value
		merge.setLanguage(coalesceQualifier(merge.getLanguage(), enrich.getLanguage()));

		// distinct countries, do not manage datainfo
		merge.setCountry(mergeQualifiers(merge.getCountry(), enrich.getCountry(), trust));

		// distinct subjects
		merge.setSubject(mergeStructuredProperties(merge.getSubject(), enrich.getSubject(), trust));

		// distinct titles
		merge.setTitle(mergeStructuredProperties(merge.getTitle(), enrich.getTitle(), trust));

		merge.setRelevantdate(mergeStructuredProperties(merge.getRelevantdate(), enrich.getRelevantdate(), trust));

		if (merge.getDescription() == null || merge.getDescription().isEmpty() || trust == 0) {
			merge.setDescription(longestLists(merge.getDescription(), enrich.getDescription()));
		}

		merge
			.setDateofacceptance(
				mergeDateOfAcceptance(merge.getDateofacceptance(), enrich.getDateofacceptance(), trust));

		merge.setPublisher(coalesce(merge.getPublisher(), enrich.getPublisher()));
		merge.setEmbargoenddate(coalesce(merge.getEmbargoenddate(), enrich.getEmbargoenddate()));
		merge.setSource(unionDistinctLists(merge.getSource(), enrich.getSource(), trust));
		merge.setFulltext(unionDistinctLists(merge.getFulltext(), enrich.getFulltext(), trust));
		merge.setFormat(unionDistinctLists(merge.getFormat(), enrich.getFormat(), trust));
		merge.setContributor(unionDistinctLists(merge.getContributor(), enrich.getContributor(), trust));

		// this field might contain the original type from the raw metadata, no strategy yet to merge it
		merge.setResourcetype(coalesce(merge.getResourcetype(), enrich.getResourcetype()));

		merge.setCoverage(unionDistinctLists(merge.getCoverage(), enrich.getCoverage(), trust));

		if (enrich.getBestaccessright() != null
			&& new AccessRightComparator<>()
				.compare(enrich.getBestaccessright(), merge.getBestaccessright()) < 0) {
			merge.setBestaccessright(enrich.getBestaccessright());
		}

		// merge datainfo for same context id
		merge.setContext(mergeLists(merge.getContext(), enrich.getContext(), trust, Context::getId, (r, l) -> {
			r.getDataInfo().addAll(l.getDataInfo());
			return r;
		}));

		// ok
		merge
			.setExternalReference(
				mergeExternalReference(merge.getExternalReference(), enrich.getExternalReference(), trust));

		// instance enrichment or union
		// review instance equals => add pid to comparision
		if (!isAnEnrichment(merge) && !isAnEnrichment(enrich)) {
			merge.setInstance(mergeInstances(merge.getInstance(), enrich.getInstance(), trust));
		} else {
			final List<Instance> enrichmentInstances = isAnEnrichment(merge) ? merge.getInstance()
				: enrich.getInstance();
			final List<Instance> enrichedInstances = isAnEnrichment(merge) ? enrich.getInstance()
				: merge.getInstance();
			if (isAnEnrichment(merge))
				merge.setDataInfo(enrich.getDataInfo());
			merge.setInstance(enrichInstances(enrichedInstances, enrichmentInstances));
		}

		merge
			.setEoscifguidelines(
				mergeEosciifguidelines(merge.getEoscifguidelines(), enrich.getEoscifguidelines(), trust));
		merge.setIsGreen(booleanOR(merge.getIsGreen(), enrich.getIsGreen()));
		// OK but should be list of values
		merge.setOpenAccessColor(coalesce(merge.getOpenAccessColor(), enrich.getOpenAccessColor()));
		merge.setIsInDiamondJournal(booleanOR(merge.getIsInDiamondJournal(), enrich.getIsInDiamondJournal()));
		merge.setPubliclyFunded(booleanOR(merge.getPubliclyFunded(), enrich.getPubliclyFunded()));

		if (StringUtils.isBlank(merge.getTransformativeAgreement())) {
			merge.setTransformativeAgreement(enrich.getTransformativeAgreement());
		}

		return merge;
	}

	private static Field<String> mergeDateOfAcceptance(Field<String> merge, Field<String> enrich, int trust) {
		// higher trust then oldest date
		if ((merge == null || trust == 0) && enrich != null) {
			if (merge == null) {
				return enrich;
			} else {
				try {
					LocalDate merge_date = LocalDate.parse(merge.getValue(), DateTimeFormatter.ISO_DATE);
					try {
						LocalDate enrich_date = LocalDate.parse(enrich.getValue(), DateTimeFormatter.ISO_DATE);

						if (enrich_date.getYear() > 1300
							&& (merge_date.getYear() < 1300 || merge_date.isAfter(enrich_date))) {
							return enrich;
						}
					} catch (NullPointerException | DateTimeParseException e) {
						return merge;
					}
				} catch (NullPointerException | DateTimeParseException e) {
					return enrich;
				}
			}
		}

		// keep value
		return merge;
	}

	private static List<Instance> mergeInstances(List<Instance> v1, List<Instance> v2, int trust) {
		return mergeLists(
			v1, v2, trust,
			MergeUtils::instanceKeyExtractor,
			MergeUtils::instanceMerger);
	}

	private static List<EoscIfGuidelines> mergeEosciifguidelines(List<EoscIfGuidelines> v1, List<EoscIfGuidelines> v2,
		int trust) {
		return mergeLists(
			v1, v2, trust, er -> Joiner
				.on("||")
				.useForNull("")
				.join(er.getCode(), er.getLabel(), er.getUrl(), er.getSemanticRelation()),
			(r, l) -> r);

	}

	private static List<ExternalReference> mergeExternalReference(List<ExternalReference> v1,
		List<ExternalReference> v2, int trust) {
		return mergeLists(
			v1, v2, trust, er -> Joiner
				.on(',')
				.useForNull("")
				.join(
					er.getSitename(), er.getLabel(),
					er.getUrl(), toString(er.getQualifier()), er.getRefidentifier(),
					er.getQuery(), toString(er.getDataInfo())),
			(r, l) -> r);
	}

	private static String toString(DataInfo di) {
		return Joiner
			.on(',')
			.useForNull("")
			.join(
				di.getInvisible(), di.getInferred(), di.getDeletedbyinference(), di.getTrust(),
				di.getInferenceprovenance(), toString(di.getProvenanceaction()));
	}

	private static String toString(Qualifier q) {
		return Joiner
			.on(',')
			.useForNull("")
			.join(q.getClassid(), q.getClassname(), q.getSchemeid(), q.getSchemename());
	}

	private static String toString(StructuredProperty sp) {
		return Joiner
			.on(',')
			.useForNull("")
			.join(toString(sp.getQualifier()), sp.getValue());
	}

	private static <T extends StructuredProperty> List<T> mergeStructuredProperties(List<T> v1, List<T> v2, int trust) {
		return mergeLists(v1, v2, trust, MergeUtils::toString, (r, l) -> r);
	}

	private static <T extends Qualifier> List<T> mergeQualifiers(List<T> v1, List<T> v2, int trust) {
		return mergeLists(v1, v2, trust, MergeUtils::toString, (r, l) -> r);
	}

	private static <T> T coalesce(T m, T e) {
		return m != null ? m : e;
	}

	private static Qualifier coalesceQualifier(Qualifier m, Qualifier e) {
		if (m == null || m.getClassid() == null || StringUtils.isBlank(m.getClassid())) {
			return e;
		}
		return m;
	}

	private static List<Author> mergeAuthors(List<Author> author, List<Author> author1, int trust) {
		List<List<Author>> authors = new ArrayList<>();
		if (author != null) {
			authors.add(author);
		}
		if (author1 != null) {
			authors.add(author1);
		}
		return AuthorMerger.merge(authors);
	}

	private static String instanceKeyExtractor(Instance i) {
		// three levels of concatenating:
		// 1. ::
		// 2. @@
		// 3. ||
		return String
			.join(
				"::",
				kvKeyExtractor(i.getHostedby()),
				kvKeyExtractor(i.getCollectedfrom()),
				qualifierKeyExtractor(i.getAccessright()),
				qualifierKeyExtractor(i.getInstancetype()),
				Optional.ofNullable(i.getUrl()).map(u -> String.join("@@", u)).orElse(null),
				Optional
					.ofNullable(i.getPid())
					.map(pp -> pp.stream().map(MergeUtils::spKeyExtractor).collect(Collectors.joining("@@")))
					.orElse(null));
	}

	private static Instance instanceMerger(Instance i1, Instance i2) {
		Instance i = new Instance();
		i.setHostedby(i1.getHostedby());
		i.setCollectedfrom(i1.getCollectedfrom());
		i.setAccessright(i1.getAccessright());
		i.setInstancetype(i1.getInstancetype());
		i.setPid(mergeLists(i1.getPid(), i2.getPid(), 0, MergeUtils::spKeyExtractor, (sp1, sp2) -> sp1));
		i
			.setAlternateIdentifier(
				mergeLists(
					i1.getAlternateIdentifier(), i2.getAlternateIdentifier(), 0, MergeUtils::spKeyExtractor,
					(sp1, sp2) -> sp1));

		i
			.setRefereed(
				Collections
					.min(
						Stream.of(i1.getRefereed(), i2.getRefereed()).collect(Collectors.toList()),
						new RefereedComparator()));
		i
			.setInstanceTypeMapping(
				mergeLists(
					i1.getInstanceTypeMapping(), i2.getInstanceTypeMapping(), 0,
					MergeUtils::instanceTypeMappingKeyExtractor, (itm1, itm2) -> itm1));
		i.setFulltext(selectFulltext(i1.getFulltext(), i2.getFulltext()));
		i.setDateofacceptance(selectOldestDate(i1.getDateofacceptance(), i2.getDateofacceptance()));
		i.setLicense(coalesce(i1.getLicense(), i2.getLicense()));
		i.setProcessingchargeamount(coalesce(i1.getProcessingchargeamount(), i2.getProcessingchargeamount()));
		i.setProcessingchargecurrency(coalesce(i1.getProcessingchargecurrency(), i2.getProcessingchargecurrency()));
		i
			.setMeasures(
				mergeLists(i1.getMeasures(), i2.getMeasures(), 0, MergeUtils::measureKeyExtractor, (m1, m2) -> m1));

		i.setUrl(unionDistinctListOfString(i1.getUrl(), i2.getUrl()));

		return i;
	}

	private static String measureKeyExtractor(Measure m) {
		return String
			.join(
				"::",
				m.getId(),
				m
					.getUnit()
					.stream()
					.map(KeyValue::getKey)
					.collect(Collectors.joining("::")));
	}

	private static Field<String> selectOldestDate(Field<String> d1, Field<String> d2) {
		if (!GraphCleaningFunctions.cleanDateField(d1).isPresent()) {
			return d2;
		} else if (!GraphCleaningFunctions.cleanDateField(d2).isPresent()) {
			return d1;
		}

		return Stream
			.of(d1, d2)
			.min(
				Comparator
					.comparing(
						f -> DateParserUtils
							.parseDate(f.getValue())
							.toInstant()
							.atZone(ZoneId.systemDefault())
							.toLocalDate()))
			.orElse(d1);
	}

	private static String selectFulltext(String ft1, String ft2) {
		if (StringUtils.endsWith(ft1, "pdf")) {
			return ft1;
		}
		if (StringUtils.endsWith(ft2, "pdf")) {
			return ft2;
		}
		return firstNonNull(ft1, ft2);
	}

	private static String instanceTypeMappingKeyExtractor(InstanceTypeMapping itm) {
		return String
			.join(
				"::",
				itm.getOriginalType(),
				itm.getTypeCode(),
				itm.getTypeLabel(),
				itm.getVocabularyName());
	}

	private static String kvKeyExtractor(KeyValue kv) {
		return Optional.ofNullable(kv).map(KeyValue::getKey).orElse(null);
	}

	private static String qualifierKeyExtractor(Qualifier q) {
		return Optional.ofNullable(q).map(Qualifier::getClassid).orElse(null);
	}

	private static <T> T fieldKeyExtractor(Field<T> f) {
		return Optional.ofNullable(f).map(Field::getValue).orElse(null);
	}

	private static String spKeyExtractor(StructuredProperty sp) {
		return Optional
			.ofNullable(sp)
			.map(
				s -> Joiner
					.on("||")
					.useForNull("")
					.join(qualifierKeyExtractor(s.getQualifier()), s.getValue()))
			.orElse(null);
	}

	private static <T extends OtherResearchProduct> T mergeORP(T original, T enrich) {
		int trust = compareTrust(original, enrich);
		final T merge = mergeResultFields(original, enrich);

		merge.setContactperson(unionDistinctLists(merge.getContactperson(), enrich.getContactperson(), trust));
		merge.setContactgroup(unionDistinctLists(merge.getContactgroup(), enrich.getContactgroup(), trust));
		merge.setTool(unionDistinctLists(merge.getTool(), enrich.getTool(), trust));

		return merge;
	}

	private static <T extends Software> T mergeSoftware(T original, T enrich) {
		int trust = compareTrust(original, enrich);
		final T merge = mergeResultFields(original, enrich);

		merge.setDocumentationUrl(unionDistinctLists(merge.getDocumentationUrl(), enrich.getDocumentationUrl(), trust));
		merge.setLicense(unionDistinctLists(merge.getLicense(), enrich.getLicense(), trust));
		merge.setCodeRepositoryUrl(chooseReference(merge.getCodeRepositoryUrl(), enrich.getCodeRepositoryUrl(), trust));
		merge
			.setProgrammingLanguage(
				chooseReference(merge.getProgrammingLanguage(), enrich.getProgrammingLanguage(), trust));

		return merge;
	}

	private static <T extends Dataset> T mergeDataset(T original, T enrich) {
		int trust = compareTrust(original, enrich);
		T merge = mergeResultFields(original, enrich);

		merge.setStoragedate(chooseReference(merge.getStoragedate(), enrich.getStoragedate(), trust));
		merge.setDevice(chooseReference(merge.getDevice(), enrich.getDevice(), trust));
		merge.setSize(chooseReference(merge.getSize(), enrich.getSize(), trust));
		merge.setVersion(chooseReference(merge.getVersion(), enrich.getVersion(), trust));
		merge
			.setLastmetadataupdate(
				chooseReference(merge.getLastmetadataupdate(), enrich.getLastmetadataupdate(), trust));
		merge
			.setMetadataversionnumber(
				chooseReference(merge.getMetadataversionnumber(), enrich.getMetadataversionnumber(), trust));
		merge.setGeolocation(unionDistinctLists(merge.getGeolocation(), enrich.getGeolocation(), trust));

		return merge;
	}

	public static <T extends Publication> T mergePublication(T original, T enrich) {
		final int trust = compareTrust(original, enrich);
		T merged = mergeResultFields(original, enrich);

		merged.setJournal(chooseReference(merged.getJournal(), enrich.getJournal(), trust));

		return merged;
	}

	private static <T extends Organization> T mergeOrganization(T left, T enrich) {
		int trust = compareTrust(left, enrich);
		T merged = mergeOafEntityFields(left, enrich, trust);

		merged.setLegalshortname(chooseReference(merged.getLegalshortname(), enrich.getLegalshortname(), trust));
		merged.setLegalname(chooseReference(merged.getLegalname(), enrich.getLegalname(), trust));
		merged
			.setAlternativeNames(unionDistinctLists(enrich.getAlternativeNames(), merged.getAlternativeNames(), trust));
		merged.setWebsiteurl(chooseReference(merged.getWebsiteurl(), enrich.getWebsiteurl(), trust));
		merged.setLogourl(chooseReference(merged.getLogourl(), enrich.getLogourl(), trust));
		merged.setEclegalbody(chooseReference(merged.getEclegalbody(), enrich.getEclegalbody(), trust));
		merged.setEclegalperson(chooseReference(merged.getEclegalperson(), enrich.getEclegalperson(), trust));
		merged.setEcnonprofit(chooseReference(merged.getEcnonprofit(), enrich.getEcnonprofit(), trust));
		merged
			.setEcresearchorganization(
				chooseReference(merged.getEcresearchorganization(), enrich.getEcresearchorganization(), trust));
		merged
			.setEchighereducation(chooseReference(merged.getEchighereducation(), enrich.getEchighereducation(), trust));
		merged
			.setEcinternationalorganizationeurinterests(
				chooseReference(
					merged.getEcinternationalorganizationeurinterests(),
					enrich.getEcinternationalorganizationeurinterests(), trust));
		merged
			.setEcinternationalorganization(
				chooseReference(
					merged.getEcinternationalorganization(), enrich.getEcinternationalorganization(), trust));
		merged.setEcenterprise(chooseReference(merged.getEcenterprise(), enrich.getEcenterprise(), trust));
		merged.setEcsmevalidated(chooseReference(merged.getEcsmevalidated(), enrich.getEcsmevalidated(), trust));
		merged.setEcnutscode(chooseReference(merged.getEcnutscode(), enrich.getEcnutscode(), trust));
		merged.setCountry(chooseReference(merged.getCountry(), enrich.getCountry(), trust));

		return merged;
	}

	public static <T extends Project> T mergeProject(T original, T enrich) {
		int trust = compareTrust(original, enrich);
		T merged = mergeOafEntityFields(original, enrich, trust);

		merged.setWebsiteurl(chooseReference(merged.getWebsiteurl(), enrich.getWebsiteurl(), trust));
		merged.setCode(chooseReference(merged.getCode(), enrich.getCode(), trust));
		merged.setAcronym(chooseReference(merged.getAcronym(), enrich.getAcronym(), trust));
		merged.setTitle(chooseReference(merged.getTitle(), enrich.getTitle(), trust));
		merged.setStartdate(chooseReference(merged.getStartdate(), enrich.getStartdate(), trust));
		merged.setEnddate(chooseReference(merged.getEnddate(), enrich.getEnddate(), trust));
		merged.setCallidentifier(chooseReference(merged.getCallidentifier(), enrich.getCallidentifier(), trust));
		merged.setKeywords(chooseReference(merged.getKeywords(), enrich.getKeywords(), trust));
		merged.setDuration(chooseReference(merged.getDuration(), enrich.getDuration(), trust));
		merged.setEcsc39(chooseReference(merged.getEcsc39(), enrich.getEcsc39(), trust));
		merged
			.setOamandatepublications(
				chooseReference(merged.getOamandatepublications(), enrich.getOamandatepublications(), trust));
		merged.setEcarticle29_3(chooseReference(merged.getEcarticle29_3(), enrich.getEcarticle29_3(), trust));
		merged.setSubjects(unionDistinctLists(merged.getSubjects(), enrich.getSubjects(), trust));
		merged.setFundingtree(unionDistinctLists(merged.getFundingtree(), enrich.getFundingtree(), trust));
		merged.setContracttype(chooseReference(merged.getContracttype(), enrich.getContracttype(), trust));
		merged.setOptional1(chooseReference(merged.getOptional1(), enrich.getOptional1(), trust));
		merged.setOptional2(chooseReference(merged.getOptional2(), enrich.getOptional2(), trust));
		merged.setJsonextrainfo(chooseReference(merged.getJsonextrainfo(), enrich.getJsonextrainfo(), trust));
		merged.setContactfullname(chooseReference(merged.getContactfullname(), enrich.getContactfullname(), trust));
		merged.setContactfax(chooseReference(merged.getContactfax(), enrich.getContactfax(), trust));
		merged.setContactphone(chooseReference(merged.getContactphone(), enrich.getContactphone(), trust));
		merged.setContactemail(chooseReference(merged.getContactemail(), enrich.getContactemail(), trust));
		merged.setSummary(chooseReference(merged.getSummary(), enrich.getSummary(), trust));
		merged.setCurrency(chooseReference(merged.getCurrency(), enrich.getCurrency(), trust));

		// missin in Project.merge
		merged.setTotalcost(chooseReference(merged.getTotalcost(), enrich.getTotalcost(), trust));
		merged.setFundedamount(chooseReference(merged.getFundedamount(), enrich.getFundedamount(), trust));

		// trust ??
		if (enrich.getH2020topiccode() != null && StringUtils.isEmpty(merged.getH2020topiccode())) {
			merged.setH2020topiccode(enrich.getH2020topiccode());
			merged.setH2020topicdescription(enrich.getH2020topicdescription());
		}

		merged
			.setH2020classification(
				unionDistinctLists(merged.getH2020classification(), enrich.getH2020classification(), trust));

		return merged;
	}

	/**
	 * Longest lists list.
	 *
	 * @param a the a
	 * @param b the b
	 * @return the list
	 */
	private static List<Field<String>> longestLists(List<Field<String>> a, List<Field<String>> b) {
		if (a == null || b == null)
			return a == null ? b : a;

		return a.size() >= b.size() ? a : b;
	}

	/**
	 * This main method apply the enrichment of the instances
	 *
	 * @param toEnrichInstances   the instances that could be enriched
	 * @param enrichmentInstances the enrichment instances
	 * @return list of instances possibly enriched
	 */
	private static List<Instance> enrichInstances(final List<Instance> toEnrichInstances,
		final List<Instance> enrichmentInstances) {
		final List<Instance> enrichmentResult = new ArrayList<>();

		if (toEnrichInstances == null) {
			return enrichmentResult;
		}

		if (enrichmentInstances == null || enrichmentInstances.isEmpty()) {
			return toEnrichInstances;
		}

		Map<String, Instance> ri = toInstanceMap(enrichmentInstances);

		toEnrichInstances.forEach(i -> {
			final List<Instance> e = findEnrichmentsByPID(i.getPid(), ri);
			if (e != null && e.size() > 0) {
				e.forEach(enr -> applyEnrichment(i, enr));
			} else {
				final List<Instance> a = findEnrichmentsByPID(i.getAlternateIdentifier(), ri);
				if (a != null && a.size() > 0) {
					a.forEach(enr -> applyEnrichment(i, enr));
				}
			}
			enrichmentResult.add(i);
		});
		return enrichmentResult;
	}

	/**
	 * This method converts the list of instance enrichments
	 * into a Map where the key is the normalized identifier
	 * and the value is the instance itself
	 *
	 * @param ri the list of enrichment instances
	 * @return the result map
	 */
	private static Map<String, Instance> toInstanceMap(final List<Instance> ri) {
		return ri
			.stream()
			.filter(i -> i.getPid() != null || i.getAlternateIdentifier() != null)
			.flatMap(i -> {
				final List<Pair<String, Instance>> result = new ArrayList<>();
				if (i.getPid() != null)
					i
						.getPid()
						.stream()
						.filter(MergeUtils::validPid)
						.forEach(p -> result.add(new ImmutablePair<>(extractKeyFromPid(p), i)));
				if (i.getAlternateIdentifier() != null)
					i
						.getAlternateIdentifier()
						.stream()
						.filter(MergeUtils::validPid)
						.forEach(p -> result.add(new ImmutablePair<>(extractKeyFromPid(p), i)));
				return result.stream();
			})
			.collect(
				Collectors
					.toMap(
						Pair::getLeft,
						Pair::getRight,
						(a, b) -> a));
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

	/**
	 * Valid pid boolean.
	 *
	 * @param p the p
	 * @return the boolean
	 */
	private static boolean validPid(final StructuredProperty p) {
		return p.getValue() != null && p.getQualifier() != null && p.getQualifier().getClassid() != null;
	}

	/**
	 * Normalize pid string.
	 *
	 * @param pid the pid
	 * @return the string
	 */
	private static String extractKeyFromPid(final StructuredProperty pid) {
		if (pid == null)
			return null;
		final StructuredProperty normalizedPid = PidCleaner.normalizePidValue(pid);

		return String.format("%s::%s", normalizedPid.getQualifier().getClassid(), normalizedPid.getValue());
	}

	/**
	 * This utility method finds the list of enrichment instances
	 * that match one or more PIDs in the input list
	 *
	 * @param pids        the list of PIDs
	 * @param enrichments the List of enrichment instances having the same pid
	 * @return the list
	 */
	private static List<Instance> findEnrichmentsByPID(final List<StructuredProperty> pids,
		final Map<String, Instance> enrichments) {
		if (pids == null || enrichments == null)
			return null;
		return pids
			.stream()
			.map(MergeUtils::extractKeyFromPid)
			.map(enrichments::get)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}

	/**
	 * Is an enrichment boolean.
	 *
	 * @param e the e
	 * @return the boolean
	 */
	private static boolean isAnEnrichment(OafEntity e) {
		return e.getDataInfo() != null &&
			e.getDataInfo().getProvenanceaction() != null
			&& ModelConstants.PROVENANCE_ENRICH.equalsIgnoreCase(e.getDataInfo().getProvenanceaction().getClassid());
	}

	/**
	 * This method apply enrichment on a single instance
	 * The enrichment consists of replacing values on
	 * single attribute only if in the current instance is missing
	 * The only repeatable field enriched is measures
	 *
	 * @param merge      the current instance
	 * @param enrichment the enrichment instance
	 */
	private static void applyEnrichment(final Instance merge, final Instance enrichment) {
		if (merge == null || enrichment == null)
			return;

		merge.setLicense(firstNonNull(merge.getLicense(), enrichment.getLicense()));
		merge.setAccessright(firstNonNull(merge.getAccessright(), enrichment.getAccessright()));
		merge.setInstancetype(firstNonNull(merge.getInstancetype(), enrichment.getInstancetype()));
		merge.setInstanceTypeMapping(firstNonNull(merge.getInstanceTypeMapping(), enrichment.getInstanceTypeMapping()));
		merge.setHostedby(firstNonNull(merge.getHostedby(), enrichment.getHostedby()));
		merge.setUrl(unionDistinctLists(merge.getUrl(), enrichment.getUrl(), 0));
		merge
			.setDistributionlocation(
				firstNonNull(merge.getDistributionlocation(), enrichment.getDistributionlocation()));
		merge.setCollectedfrom(firstNonNull(merge.getCollectedfrom(), enrichment.getCollectedfrom()));
		// pid and alternateId are used for matching
		merge.setDateofacceptance(firstNonNull(merge.getDateofacceptance(), enrichment.getDateofacceptance()));
		merge
			.setProcessingchargeamount(
				firstNonNull(merge.getProcessingchargeamount(), enrichment.getProcessingchargeamount()));
		merge
			.setProcessingchargecurrency(
				firstNonNull(merge.getProcessingchargecurrency(), enrichment.getProcessingchargecurrency()));
		merge.setRefereed(firstNonNull(merge.getRefereed(), enrichment.getRefereed()));
		merge.setMeasures(unionDistinctLists(merge.getMeasures(), enrichment.getMeasures(), 0));
		merge.setFulltext(firstNonNull(merge.getFulltext(), enrichment.getFulltext()));
	}

	private static int compareTrust(Oaf a, Oaf b) {
		String left = Optional
			.ofNullable(a.getDataInfo())
			.map(DataInfo::getTrust)
			.orElse("0.0");

		String right = Optional
			.ofNullable(b.getDataInfo())
			.map(DataInfo::getTrust)
			.orElse("0.0");

		return left.compareTo(right);
	}

}
