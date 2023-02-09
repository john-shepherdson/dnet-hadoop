
package eu.dnetlib.dhp.schema.oaf.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.dnetlib.dhp.schema.oaf.common.ModelSupport.isSubClass;
import static eu.dnetlib.dhp.schema.oaf.common.ModelSupport.sameClass;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.common.AccessRightComparator;
import eu.dnetlib.dhp.schema.oaf.common.ModelSupport;

public class MergeUtils {

	public static <T extends Oaf> T merge(final T left, final T right) {
		if (sameClass(left, right, Entity.class)) {
			return mergeEntities(left, right);
		} else if (sameClass(left, right, Relation.class)) {
			return mergeRelation(left, right);
		} else {
			throw new RuntimeException(
					String
							.format(
									"MERGE_FROM_AND_GET incompatible types: %s, %s",
									left.getClass().getCanonicalName(), right.getClass().getCanonicalName()));
		}
	}

	private static <T extends Oaf> T mergeEntities(T left, T right) {
		if (sameClass(left, right, Result.class)) {
			if (!left.getClass().equals(right.getClass())) {
				return mergeResultsOfDifferentTypes(left, right);
			}
			return mergeResult(left, right);
		} else if (sameClass(left, right, Datasource.class)) {
			// TODO
			return left;
		} else if (sameClass(left, right, Organization.class)) {
			return mergeOrganization(left, right);
		} else if (sameClass(left, right, Project.class)) {
			return mergeProject(left, right);
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
	 *
	 * Otherwise, it considers a resulttype priority order implemented in {@link ResultTypeComparator}
	 * and proceeds with the canonical property merging.
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	private static <T extends Oaf> T mergeResultsOfDifferentTypes(T left, T right) {

		final boolean leftFromDelegatedAuthority = isFromDelegatedAuthority((Result) left);
		final boolean rightFromDelegatedAuthority = isFromDelegatedAuthority((Result) right);

		if (leftFromDelegatedAuthority && !rightFromDelegatedAuthority) {
			return left;
		}
		if (!leftFromDelegatedAuthority && rightFromDelegatedAuthority) {
			return right;
		}
		if (new ResultTypeComparator().compare((Result) left, (Result) right) < 0) {
			return mergeResult(left, right);
		} else {
			return mergeResult(right, left);
		}
	}

	/**
	 * Internal utility that merges the common entity fields
	 *
	 * @param left
	 * @param right
	 * @return
	 * @param <T>
	 */
	private static <T extends Oaf> T mergeEntityFields(T left, T right) {

		final Entity enrich = (Entity) right;
		final Entity mergedEntity = (Entity) left;

		mergedEntity.setOriginalId(mergeLists(mergedEntity.getOriginalId(), enrich.getOriginalId()));
		mergedEntity.setCollectedfrom(mergeLists(mergedEntity.getCollectedfrom(), enrich.getCollectedfrom()));

		if (mergedEntity.getLastupdatetimestamp() == null && enrich.getLastupdatetimestamp() != null) {
			mergedEntity.setLastupdatetimestamp(enrich.getLastupdatetimestamp());
		} else if (mergedEntity.getLastupdatetimestamp() != null && enrich.getLastupdatetimestamp() != null) {
			mergedEntity
					.setLastupdatetimestamp(
							Long.max(mergedEntity.getLastupdatetimestamp(), enrich.getLastupdatetimestamp()));
		}

		mergedEntity.setPid(mergeLists(mergedEntity.getPid(), enrich.getPid()));

		final int trustCompareResult = compareTrust(mergedEntity, enrich);
		if (enrich.getDateofcollection() != null && trustCompareResult < 0)
			mergedEntity.setDateofcollection(enrich.getDateofcollection());

		if (enrich.getDateoftransformation() != null && trustCompareResult < 0)
			mergedEntity.setDateoftransformation(enrich.getDateoftransformation());

		mergedEntity.setMeasures(mergeLists(mergedEntity.getMeasures(), enrich.getMeasures()));
		mergedEntity.setExtraInfo(mergeLists(mergedEntity.getExtraInfo(), enrich.getExtraInfo()));

		return (T) mergedEntity;
	}

	private static <T extends Oaf> T mergeRelation(T left, T right) {

		Relation original = (Relation) left;
		Relation enrich = (Relation) right;

		checkArgument(Objects.equals(original.getSource(), enrich.getSource()), "source ids must be equal");
		checkArgument(Objects.equals(original.getTarget(), enrich.getTarget()), "target ids must be equal");
		checkArgument(Objects.equals(original.getRelType(), enrich.getRelType()), "relType(s) must be equal");
		checkArgument(
				Objects.equals(original.getSubRelType(), enrich.getSubRelType()), "subRelType(s) must be equal");
		checkArgument(Objects.equals(original.getRelClass(), enrich.getRelClass()), "relClass(es) must be equal");

		original.setProvenance(mergeLists(original.getProvenance(), enrich.getProvenance()));

		original.setValidated(original.getValidated() || enrich.getValidated());
		try {
			original.setValidationDate(ModelSupport.oldest(original.getValidationDate(), enrich.getValidationDate()));
		} catch (ParseException e) {
			throw new IllegalArgumentException(String
					.format(
							"invalid validation date format in relation [s:%s, t:%s]: %s", original.getSource(),
							original.getTarget(),
							original.getValidationDate()));
		}

		return (T) original;
	}

	private static <T extends Oaf> T mergeResult(T left, T right) {

		Result original = (Result) left;
		Result enrich = (Result) right;

		final Result mergedResult = mergeEntityFields(original, enrich);

		if (StringUtils.isBlank(mergedResult.getProcessingchargeamount())) {
			mergedResult.setProcessingchargeamount(enrich.getProcessingchargeamount());
			mergedResult.setProcessingchargecurrency(enrich.getProcessingchargecurrency());
		}

		mergedResult.setMeasures(mergeLists(mergedResult.getMeasures(), enrich.getMeasures()));

		if (!isAnEnrichment(mergedResult) && !isAnEnrichment(enrich))
			mergedResult.setInstance(mergeLists(mergedResult.getInstance(), enrich.getInstance()));
		else {
			final List<Instance> enrichmentInstances = isAnEnrichment(mergedResult) ? mergedResult.getInstance()
				: enrich.getInstance();
			final List<Instance> enrichedInstances = isAnEnrichment(mergedResult) ? enrich.getInstance()
				: mergedResult.getInstance();
			if (isAnEnrichment(mergedResult))
				mergedResult.setDataInfo(enrich.getDataInfo());
			mergedResult.setInstance(enrichInstances(enrichedInstances, enrichmentInstances));
		}

		if (enrich.getBestaccessright() != null
			&& new AccessRightComparator<>()
				.compare(enrich.getBestaccessright(), mergedResult.getBestaccessright()) < 0)
			mergedResult.setBestaccessright(enrich.getBestaccessright());

		final int trustCompareResult = compareTrust(mergedResult, enrich);

		if (enrich.getResulttype() != null && trustCompareResult < 0)
			mergedResult.setResulttype(enrich.getResulttype());

		if (enrich.getLanguage() != null && trustCompareResult < 0)
			mergedResult.setLanguage(enrich.getLanguage());

		if (Objects.nonNull(enrich.getDateofacceptance())) {
			if (Objects.isNull(mergedResult.getDateofacceptance()) || trustCompareResult < 0) {
				mergedResult.setDateofacceptance(enrich.getDateofacceptance());
			}
		}

		mergedResult.setCountry(mergeLists(mergedResult.getCountry(), enrich.getCountry()));

		mergedResult.setSubject(mergeLists(mergedResult.getSubject(), enrich.getSubject()));

		if (enrich.getJournal() != null && trustCompareResult < 0)
			mergedResult.setJournal(enrich.getJournal());

		// merge title lists: main title with higher trust and distinct between the others
		StructuredProperty baseMainTitle = null;
		if (mergedResult.getTitle() != null) {
			baseMainTitle = getMainTitle(mergedResult.getTitle());
			if (baseMainTitle != null) {
				final StructuredProperty p = baseMainTitle;
				mergedResult
					.setTitle(mergedResult.getTitle().stream().filter(t -> t != p).collect(Collectors.toList()));
			}
		}

		StructuredProperty newMainTitle = null;
		if (enrich.getTitle() != null) {
			newMainTitle = getMainTitle(enrich.getTitle());
			if (newMainTitle != null) {
				final StructuredProperty p = newMainTitle;
				enrich.setTitle(enrich.getTitle().stream().filter(t -> t != p).collect(Collectors.toList()));
			}
		}

		if (newMainTitle != null && trustCompareResult < 0) {
			baseMainTitle = newMainTitle;
		}

		mergedResult.setTitle(mergeLists(mergedResult.getTitle(), enrich.getTitle()));
		if (mergedResult.getTitle() != null && baseMainTitle != null) {
			mergedResult.getTitle().add(baseMainTitle);
		}

		mergedResult.setRelevantdate(mergeLists(mergedResult.getRelevantdate(), enrich.getRelevantdate()));

		mergedResult.setDescription(longestLists(mergedResult.getDescription(), enrich.getDescription()));

		if (enrich.getPublisher() != null && trustCompareResult < 0)
			mergedResult.setPublisher(enrich.getPublisher());

		if (enrich.getEmbargoenddate() != null && trustCompareResult < 0)
			mergedResult.setEmbargoenddate(enrich.getEmbargoenddate());

		mergedResult.setSource(mergeLists(mergedResult.getSource(), enrich.getSource()));

		mergedResult.setFulltext(mergeLists(mergedResult.getFulltext(), enrich.getFulltext()));

		mergedResult.setFormat(mergeLists(mergedResult.getFormat(), enrich.getFormat()));

		mergedResult.setContributor(mergeLists(mergedResult.getContributor(), enrich.getContributor()));

		if (enrich.getResourcetype() != null)
			mergedResult.setResourcetype(enrich.getResourcetype());

		mergedResult.setCoverage(mergeLists(mergedResult.getCoverage(), enrich.getCoverage()));

		mergedResult.setContext(mergeLists(mergedResult.getContext(), enrich.getContext()));

		mergedResult
			.setExternalReference(mergeLists(mergedResult.getExternalReference(), enrich.getExternalReference()));

		if (enrich.getOaiprovenance() != null && trustCompareResult < 0)
			mergedResult.setOaiprovenance(enrich.getOaiprovenance());

		if (isSubClass(mergedResult, Publication.class)) {
			return (T) mergePublication(mergedResult, enrich);
		}
		if (isSubClass(mergedResult, Dataset.class)) {
			return (T) mergeDataset(mergedResult, enrich);
		}
		if (isSubClass(mergedResult, OtherResearchProduct.class)) {
			return (T) mergeORP(mergedResult, enrich);
		}
		if (isSubClass(mergedResult, Software.class)) {
			return (T) mergeSoftware(mergedResult, enrich);
		}

		mergeEntityDataInfo(original, enrich);

		return (T) mergedResult;
	}

	private static <T extends Oaf> T mergeORP(T left, T right) {

		final OtherResearchProduct original = (OtherResearchProduct) left;
		final OtherResearchProduct enrich = (OtherResearchProduct) right;

		original.setContactperson(mergeLists(original.getContactperson(), enrich.getContactperson()));
		original.setContactgroup(mergeLists(original.getContactgroup(), enrich.getContactgroup()));
		original.setTool(mergeLists(original.getTool(), enrich.getTool()));

		mergeEntityDataInfo(original, enrich);

		return (T) original;
	}

	private static <T extends Oaf> T mergeSoftware(T left, T right) {
		final Software original = (Software) left;
		final Software enrich = (Software) right;

		original
			.setDocumentationUrl(mergeLists(original.getDocumentationUrl(), enrich.getDocumentationUrl()));

		original
			.setCodeRepositoryUrl(
				enrich.getCodeRepositoryUrl() != null && compareTrust(original, enrich) < 0
					? enrich.getCodeRepositoryUrl()
					: original.getCodeRepositoryUrl());

		original
			.setProgrammingLanguage(
				enrich.getProgrammingLanguage() != null && compareTrust(original, enrich) < 0
					? enrich.getProgrammingLanguage()
					: original.getProgrammingLanguage());

		mergeEntityDataInfo(original, enrich);

		return (T) original;
	}

	private static <T extends Oaf> T mergeDataset(T left, T right) {
		Dataset original = (Dataset) left;
		Dataset enrich = (Dataset) right;

		original
			.setStoragedate(
				enrich.getStoragedate() != null && compareTrust(original, enrich) < 0 ? enrich.getStoragedate()
					: original.getStoragedate());

		original
			.setDevice(
				enrich.getDevice() != null && compareTrust(original, enrich) < 0 ? enrich.getDevice()
					: original.getDevice());

		original
			.setSize(
				enrich.getSize() != null && compareTrust(original, enrich) < 0 ? enrich.getSize()
					: original.getSize());

		original
			.setVersion(
				enrich.getVersion() != null && compareTrust(original, enrich) < 0 ? enrich.getVersion()
					: original.getVersion());

		original
			.setLastmetadataupdate(
				enrich.getLastmetadataupdate() != null && compareTrust(original, enrich) < 0
					? enrich.getLastmetadataupdate()
					: original.getLastmetadataupdate());

		original
			.setMetadataversionnumber(
				enrich.getMetadataversionnumber() != null && compareTrust(original, enrich) < 0
					? enrich.getMetadataversionnumber()
					: original.getMetadataversionnumber());

		original.setGeolocation(mergeLists(original.getGeolocation(), enrich.getGeolocation()));

		mergeEntityDataInfo(original, enrich);

		return (T) original;
	}

	private static <T extends Oaf> T mergePublication(T original, T enrich) {

		//add publication specific fields.

		mergeEntityDataInfo(original, enrich);

		return original;
	}

	private static <T extends Oaf> T mergeOrganization(T left, T right) {

		Organization original = (Organization) left;
		Organization enrich = (Organization) right;

		final Organization mergedOrganization = mergeEntityFields(original, enrich);

		int ct = compareTrust(mergedOrganization, enrich);
		mergedOrganization
			.setLegalshortname(
				enrich.getLegalshortname() != null && ct < 0
					? enrich.getLegalshortname()
					: mergedOrganization.getLegalname());

		mergedOrganization
			.setLegalname(
				enrich.getLegalname() != null && ct < 0 ? enrich.getLegalname()
					: mergedOrganization.getLegalname());

		mergedOrganization
			.setAlternativeNames(mergeLists(enrich.getAlternativeNames(), mergedOrganization.getAlternativeNames()));

		mergedOrganization
			.setWebsiteurl(
				enrich.getWebsiteurl() != null && ct < 0
					? enrich.getWebsiteurl()
					: mergedOrganization.getWebsiteurl());

		mergedOrganization
			.setLogourl(
				enrich.getLogourl() != null && ct < 0
					? enrich.getLogourl()
					: mergedOrganization.getLogourl());

		mergedOrganization
			.setEclegalbody(
				enrich.getEclegalbody() != null && ct < 0
					? enrich.getEclegalbody()
					: mergedOrganization.getEclegalbody());

		mergedOrganization
			.setEclegalperson(
				enrich.getEclegalperson() != null && ct < 0
					? enrich.getEclegalperson()
					: mergedOrganization.getEclegalperson());

		mergedOrganization
			.setEcnonprofit(
				enrich.getEcnonprofit() != null && ct < 0
					? enrich.getEcnonprofit()
					: mergedOrganization.getEcnonprofit());

		mergedOrganization
			.setEcresearchorganization(
				enrich.getEcresearchorganization() != null && ct < 0
					? enrich.getEcresearchorganization()
					: mergedOrganization.getEcresearchorganization());

		mergedOrganization
			.setEchighereducation(
				enrich.getEchighereducation() != null && ct < 0
					? enrich.getEchighereducation()
					: mergedOrganization.getEchighereducation());

		mergedOrganization
			.setEcinternationalorganizationeurinterests(
				enrich.getEcinternationalorganizationeurinterests() != null && ct < 0
					? enrich.getEcinternationalorganizationeurinterests()
					: mergedOrganization.getEcinternationalorganizationeurinterests());

		mergedOrganization
			.setEcinternationalorganization(
				enrich.getEcinternationalorganization() != null && ct < 0
					? enrich.getEcinternationalorganization()
					: mergedOrganization.getEcinternationalorganization());

		mergedOrganization
			.setEcenterprise(
				enrich.getEcenterprise() != null && ct < 0
					? enrich.getEcenterprise()
					: mergedOrganization.getEcenterprise());

		mergedOrganization
			.setEcsmevalidated(
				enrich.getEcsmevalidated() != null && ct < 0
					? enrich.getEcsmevalidated()
					: mergedOrganization.getEcsmevalidated());
		mergedOrganization
			.setEcnutscode(
				enrich.getEcnutscode() != null && ct < 0
					? enrich.getEcnutscode()
					: mergedOrganization.getEcnutscode());

		mergedOrganization
			.setCountry(
				enrich.getCountry() != null && ct < 0 ? enrich.getCountry()
					: mergedOrganization.getCountry());

		mergeEntityDataInfo(mergedOrganization, enrich);

		return (T) mergedOrganization;
	}

	public static <T extends Oaf> T mergeProject(T left, T right) {

		Project original = (Project) left;
		Project enrich = (Project) right;

		final Project mergedProject = mergeEntityFields(original, enrich);

		int ct = compareTrust(mergedProject, enrich);

		mergedProject
			.setWebsiteurl(
				enrich.getWebsiteurl() != null && ct < 0
					? enrich.getWebsiteurl()
					: mergedProject.getWebsiteurl());

		mergedProject.setCode(enrich.getCode() != null && ct < 0 ? enrich.getCode() : mergedProject.getCode());

		mergedProject
			.setAcronym(
				enrich.getAcronym() != null && ct < 0
					? enrich.getAcronym()
					: mergedProject.getAcronym());

		mergedProject
			.setTitle(
				enrich.getTitle() != null && ct < 0
					? enrich.getTitle()
					: mergedProject.getTitle());
		mergedProject
			.setStartdate(
				enrich.getStartdate() != null && ct < 0
					? enrich.getStartdate()
					: mergedProject.getStartdate());
		mergedProject
			.setEnddate(
				enrich.getEnddate() != null && ct < 0
					? enrich.getEnddate()
					: mergedProject.getEnddate());
		mergedProject
			.setCallidentifier(
				enrich.getCallidentifier() != null && ct < 0
					? enrich.getCallidentifier()
					: mergedProject.getCallidentifier());
		mergedProject
			.setKeywords(
				enrich.getKeywords() != null && ct < 0
					? enrich.getKeywords()
					: mergedProject.getKeywords());

		mergedProject
			.setDuration(
				enrich.getDuration() != null && ct < 0
					? enrich.getDuration()
					: mergedProject.getDuration());
		mergedProject
			.setEcsc39(
				enrich.getEcsc39() != null && ct < 0
					? enrich.getEcsc39()
					: mergedProject.getEcsc39());
		mergedProject
			.setOamandatepublications(
				enrich.getOamandatepublications() != null && ct < 0
					? enrich.getOamandatepublications()
					: mergedProject.getOamandatepublications());
		mergedProject
			.setEcarticle29_3(
				enrich.getEcarticle29_3() != null && ct < 0
					? enrich.getEcarticle29_3()
					: mergedProject.getEcarticle29_3());

		mergedProject.setSubjects(mergeLists(mergedProject.getSubjects(), enrich.getSubjects()));
		mergedProject.setFundingtree(mergeLists(mergedProject.getFundingtree(), enrich.getFundingtree()));
		mergedProject
			.setContracttype(
				enrich.getContracttype() != null && ct < 0
					? enrich.getContracttype()
					: mergedProject.getContracttype());
		mergedProject
			.setOptional1(
				enrich.getOptional1() != null && ct < 0
					? enrich.getOptional1()
					: mergedProject.getOptional1());
		mergedProject
			.setOptional2(
				enrich.getOptional2() != null && ct < 0
					? enrich.getOptional2()
					: mergedProject.getOptional2());

		mergedProject
			.setJsonextrainfo(
				enrich.getJsonextrainfo() != null && ct < 0
					? enrich.getJsonextrainfo()
					: mergedProject.getJsonextrainfo());

		mergedProject
			.setContactfullname(
				enrich.getContactfullname() != null && ct < 0
					? enrich.getContactfullname()
					: mergedProject.getContactfullname());

		mergedProject
			.setContactfax(
				enrich.getContactfax() != null && ct < 0
					? enrich.getContactfax()
					: mergedProject.getContactfax());

		mergedProject
			.setContactphone(
				enrich.getContactphone() != null && ct < 0
					? enrich.getContactphone()
					: mergedProject.getContactphone());

		mergedProject
			.setContactemail(
				enrich.getContactemail() != null && ct < 0
					? enrich.getContactemail()
					: mergedProject.getContactemail());

		mergedProject
			.setSummary(
				enrich.getSummary() != null && ct < 0
					? enrich.getSummary()
					: mergedProject.getSummary());

		mergedProject
			.setCurrency(
				enrich.getCurrency() != null && ct < 0
					? enrich.getCurrency()
					: mergedProject.getCurrency());

		if (enrich.getH2020topiccode() != null && StringUtils.isEmpty(mergedProject.getH2020topiccode())) {
			mergedProject.setH2020topiccode(enrich.getH2020topiccode());
			mergedProject.setH2020topicdescription(enrich.getH2020topicdescription());
		}

		mergedProject
			.setH2020classification(
				mergeLists(mergedProject.getH2020classification(), enrich.getH2020classification()));

		mergeEntityDataInfo(mergedProject, enrich);

		return (T) mergedProject;
	}

	private static <T extends Oaf> void mergeEntityDataInfo(T left, T right) {
		Entity l = (Entity) left;
		Entity r = (Entity) right;
		Optional
			.ofNullable(r)
			.ifPresent(
				other -> Optional
					.ofNullable(other.getDataInfo())
					.ifPresent(
						otherDataInfo -> Optional
							.ofNullable(l.getDataInfo())
							.ifPresent(thisDataInfo -> {
								if (compareTrust(r, other) < 0 || thisDataInfo.getInvisible()) {
									l.setDataInfo(otherDataInfo);
								}
							})));
	}

	/**
	 * Gets main title.
	 *
	 * @param titles the titles
	 * @return the main title
	 */
	private static StructuredProperty getMainTitle(List<StructuredProperty> titles) {
		// need to check if the list of titles contains more than 1 main title? (in that case, we should chose which
		// main title select in the list)
		for (StructuredProperty t : titles) {
			if (t.getQualifier() != null && t.getQualifier().getClassid() != null)
				if (t.getQualifier().getClassid().equals("main title"))
					return t;
		}
		return null;
	}

	/**
	 * Longest lists list.
	 *
	 * @param a the a
	 * @param b the b
	 * @return the list
	 */
	public static List<String> longestLists(List<String> a, List<String> b) {
		if (a == null || b == null)
			return a == null ? b : a;
		if (a.size() == b.size()) {
			int msa = a
				.stream()
				.filter(i -> i != null)
				.map(i -> i.length())
				.max(Comparator.naturalOrder())
				.orElse(0);
			int msb = b
				.stream()
				.filter(i -> i != null)
				.map(i -> i.length())
				.max(Comparator.naturalOrder())
				.orElse(0);
			return msa > msb ? a : b;
		}
		return a.size() > b.size() ? a : b;
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
		if (enrichmentInstances == null) {
			return enrichmentResult;
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
		final StructuredProperty normalizedPid = CleaningFunctions.normalizePidValue(pid);

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
	public static boolean isAnEnrichment(Entity e) {
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
	 * @param currentInstance the current instance
	 * @param enrichment      the enrichment instance
	 */
	private static void applyEnrichment(final Instance currentInstance, final Instance enrichment) {
		if (currentInstance == null || enrichment == null)
			return;

		// ENRICH accessright
		if (enrichment.getAccessright() != null && currentInstance.getAccessright() == null)
			currentInstance.setAccessright(enrichment.getAccessright());

		// ENRICH license
		if (enrichment.getLicense() != null && currentInstance.getLicense() == null)
			currentInstance.setLicense(enrichment.getLicense());

		// ENRICH instanceType
		if (enrichment.getInstancetype() != null && currentInstance.getInstancetype() == null)
			currentInstance.setInstancetype(enrichment.getInstancetype());

		// ENRICH hostedby
		if (enrichment.getHostedby() != null && currentInstance.getHostedby() == null)
			currentInstance.setHostedby(enrichment.getHostedby());

		// ENRICH distributionlocation
		if (enrichment.getDistributionlocation() != null && currentInstance.getDistributionlocation() == null)
			currentInstance.setDistributionlocation(enrichment.getDistributionlocation());

		// ENRICH collectedfrom
		if (enrichment.getCollectedfrom() != null && currentInstance.getCollectedfrom() == null)
			currentInstance.setCollectedfrom(enrichment.getCollectedfrom());

		// ENRICH dateofacceptance
		if (enrichment.getDateofacceptance() != null && currentInstance.getDateofacceptance() == null)
			currentInstance.setDateofacceptance(enrichment.getDateofacceptance());

		// ENRICH processingchargeamount
		if (enrichment.getProcessingchargeamount() != null && currentInstance.getProcessingchargeamount() == null)
			currentInstance.setProcessingchargeamount(enrichment.getProcessingchargeamount());

		// ENRICH refereed
		if (enrichment.getRefereed() != null && currentInstance.getRefereed() == null)
			currentInstance.setRefereed(enrichment.getRefereed());

		// TODO check the other Instance fields
	}

	private static <T> List<T> mergeLists(final List<T>... lists) {
		return Arrays
			.stream(lists)
			.filter(Objects::nonNull)
			.flatMap(List::stream)
			.filter(Objects::nonNull)
			.distinct()
			.collect(Collectors.toList());
	}

	private static int compareTrust(Entity a, Entity b) {
		return Float
			.compare(
				Optional
					.ofNullable(a.getDataInfo())
					.map(DataInfo::getTrust)
					.orElse(0f),
				Optional
					.ofNullable(b.getDataInfo())
					.map(DataInfo::getTrust)
					.orElse(0f));
	}

}
