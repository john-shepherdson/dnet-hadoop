
package eu.dnetlib.dhp.schema.oaf.utils;

import eu.dnetlib.dhp.schema.common.AccessRightComparator;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;

public class MergeUtils {

    public static <T extends Oaf> T checkedMerge(final T left, final T right) {
        return (T) merge(left, right, false);
    }

    public static Oaf merge(final Oaf left, final Oaf right) {
        return merge(left, right, false);
    }

    public static Oaf merge(final Oaf left, final Oaf right, boolean checkDelegatedAuthority) {
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
            if (!left.getClass().equals(right.getClass()) || checkDelegatedAuthority) {
                return mergeResultsOfDifferentTypes((Result)left, (Result) right);
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

            return mergeResult((Result) left, (Result) right);
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
     * Otherwise, it considers a resulttype priority order implemented in {@link ResultTypeComparator}
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
        //TODO: raise trust to have preferred fields from one or the other??
        if (new ResultTypeComparator().compare(left, right) < 0) {
            return mergeResult(left, right);
        } else {
            return mergeResult(right, left);
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


    private static <T> List<T> unionDistinctLists(final List<T> left, final List<T> right, int trust) {
        if (left == null) {
            return right;
        } else if (right == null) {
            return left;
        }

        List<T> h = trust >= 0 ? left : right;
        List<T> l = trust >= 0 ? right : left;

        return Stream.concat(h.stream(), l.stream())
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

        return Stream.concat(l.stream(), r.stream())
                .filter(StringUtils::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
    }

    //TODO review
    private static List<KeyValue> mergeKeyValue(List<KeyValue> left, List<KeyValue> right, int trust) {
        if (trust < 0) {
            List<KeyValue> s = left;
            left = right;
            right = s;
        }

        HashMap<String, KeyValue> values = new HashMap<>();
        left.forEach(kv -> values.put(kv.getKey(), kv));
        right.forEach(kv -> values.putIfAbsent(kv.getKey(), kv));

        return new ArrayList<>(values.values());
    }

    private static List<StructuredProperty> unionTitle(List<StructuredProperty> left, List<StructuredProperty> right, int trust) {
        if (left == null) {
            return right;
        } else if (right == null) {
            return left;
        }

        List<StructuredProperty> h = trust >= 0 ? left : right;
        List<StructuredProperty> l = trust >= 0 ? right : left;

        return Stream.concat(h.stream(), l.stream())
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

        //TODO: union of all values, but what does it mean with KeyValue pairs???
        merged.setCollectedfrom(mergeKeyValue(merged.getCollectedfrom(), enrich.getCollectedfrom(), trust));
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
        merged.setPid(unionDistinctLists(merged.getPid(), enrich.getPid(), trust));
        // dateofcollection mettere today quando si fa merge
        merged.setDateofcollection(chooseString(merged.getDateofcollection(), enrich.getDateofcollection(), trust));
        // setDateoftransformation mettere vuota in dedup, nota per Claudio
        merged.setDateoftransformation(chooseString(merged.getDateoftransformation(), enrich.getDateoftransformation(), trust));
        // TODO: was missing in OafEntity.merge
        merged.setExtraInfo(unionDistinctLists(merged.getExtraInfo(), enrich.getExtraInfo(), trust));
        //oaiprovenanze da mettere a null quando si genera merge
        merged.setOaiprovenance(chooseReference(merged.getOaiprovenance(), enrich.getOaiprovenance(), trust));
        merged.setMeasures(unionDistinctLists(merged.getMeasures(), enrich.getMeasures(), trust));

        return merged;
    }


    public static <T extends Relation>  T mergeRelation(T original, T enrich) {
        int trust = compareTrust(original, enrich);
        T merge = mergeOafFields(original, enrich, trust);

        checkArgument(Objects.equals(merge.getSource(), enrich.getSource()), "source ids must be equal");
        checkArgument(Objects.equals(merge.getTarget(), enrich.getTarget()), "target ids must be equal");
        checkArgument(Objects.equals(merge.getRelType(), enrich.getRelType()), "relType(s) must be equal");
        checkArgument(
                Objects.equals(merge.getSubRelType(), enrich.getSubRelType()), "subRelType(s) must be equal");
        checkArgument(Objects.equals(merge.getRelClass(), enrich.getRelClass()), "relClass(es) must be equal");

        //merge.setProvenance(mergeLists(merge.getProvenance(), enrich.getProvenance()));

        //TODO: trust ??
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
        merge.setProperties(mergeKeyValue(merge.getProperties(), enrich.getProperties(), trust));

        return merge;
    }

    public static <T extends Result> T mergeResult(T original, T enrich) {
        final int trust = compareTrust(original, enrich);
        T merge = mergeOafEntityFields(original, enrich, trust);

        if (merge.getProcessingchargeamount() == null || StringUtils.isBlank(merge.getProcessingchargeamount().getValue())) {
            merge.setProcessingchargeamount(enrich.getProcessingchargeamount());
            merge.setProcessingchargecurrency(enrich.getProcessingchargecurrency());
        }

        // author = usare la stessa logica che in dedup
        merge.setAuthor(chooseReference(merge.getAuthor(), enrich.getAuthor(), trust));
        // il primo che mi arriva secondo l'ordinamento per priorita'
        merge.setResulttype(chooseReference(merge.getResulttype(), enrich.getResulttype(), trust));
        // gestito come il resulttype perche' e' un subtype
        merge.setMetaResourceType(chooseReference(merge.getMetaResourceType(), enrich.getMetaResourceType(), trust));
        // spostiamo nell'instance e qui prendo il primo che arriva
        merge.setLanguage(chooseReference(merge.getLanguage(), enrich.getLanguage(), trust));
        // country lasicamo,o cosi' -> parentesi sul datainfo
        merge.setCountry(unionDistinctLists(merge.getCountry(), enrich.getCountry(), trust));
        //ok
        merge.setSubject(unionDistinctLists(merge.getSubject(), enrich.getSubject(), trust));
        // union per priority quindi vanno in append
        merge.setTitle(unionTitle(merge.getTitle(), enrich.getTitle(), trust));
        //ok
        merge.setRelevantdate(unionDistinctLists(merge.getRelevantdate(), enrich.getRelevantdate(), trust));
        // prima trust e poi longest list
        merge.setDescription(longestLists(merge.getDescription(), enrich.getDescription()));
        // trust piu' alto e poi piu' vecchia
        merge.setDateofacceptance(chooseReference(merge.getDateofacceptance(), enrich.getDateofacceptance(), trust));
        // ok, ma publisher va messo ripetibile
        merge.setPublisher(chooseReference(merge.getPublisher(), enrich.getPublisher(), trust));
        // ok
        merge.setEmbargoenddate(chooseReference(merge.getEmbargoenddate(), enrich.getEmbargoenddate(), trust));
        // ok
        merge.setSource(unionDistinctLists(merge.getSource(), enrich.getSource(), trust));
        // ok
        merge.setFulltext(unionDistinctLists(merge.getFulltext(), enrich.getFulltext(), trust));
        // ok
        merge.setFormat(unionDistinctLists(merge.getFormat(), enrich.getFormat(), trust));
        // ok
        merge.setContributor(unionDistinctLists(merge.getContributor(), enrich.getContributor(), trust));

        // prima prendo l'higher trust, su questo prendo il valore migliore nelle istanze TODO
        // trust maggiore ma a parita' di trust il piu' specifico (base del vocabolario)
        // vedi note
        merge.setResourcetype(firstNonNull(merge.getResourcetype(), enrich.getResourcetype()));

        // ok
        merge.setCoverage(unionDistinctLists(merge.getCoverage(), enrich.getCoverage(), trust));

        // most open ok
        if (enrich.getBestaccessright() != null
                && new AccessRightComparator<>()
                .compare(enrich.getBestaccessright(), merge.getBestaccessright()) < 0) {
            merge.setBestaccessright(enrich.getBestaccessright());
        }

        // TODO merge of datainfo given same id
        merge.setContext(unionDistinctLists(merge.getContext(), enrich.getContext(), trust));

        //ok
        merge.setExternalReference(unionDistinctLists(merge.getExternalReference(), enrich.getExternalReference(), trust));

        //instance enrichment or union
        // review instance equals => add pid to comparision
        if (!isAnEnrichment(merge) && !isAnEnrichment(enrich))
            merge.setInstance(unionDistinctLists(merge.getInstance(), enrich.getInstance(), trust));
        else {
            final List<Instance> enrichmentInstances = isAnEnrichment(merge) ? merge.getInstance()
                    : enrich.getInstance();
            final List<Instance> enrichedInstances = isAnEnrichment(merge) ? enrich.getInstance()
                    : merge.getInstance();
            if (isAnEnrichment(merge))
                merge.setDataInfo(enrich.getDataInfo());
            merge.setInstance(enrichInstances(enrichedInstances, enrichmentInstances));
        }

        merge.setEoscifguidelines(unionDistinctLists(merge.getEoscifguidelines(), enrich.getEoscifguidelines(), trust));
        merge.setIsGreen(booleanOR(merge.getIsGreen(), enrich.getIsGreen()));
        // OK but should be list of values
        merge.setOpenAccessColor(chooseReference(merge.getOpenAccessColor(), enrich.getOpenAccessColor(), trust));
        merge.setIsInDiamondJournal(booleanOR(merge.getIsInDiamondJournal(), enrich.getIsInDiamondJournal()));
        merge.setPubliclyFunded(booleanOR(merge.getPubliclyFunded(), enrich.getPubliclyFunded()));

        return merge;
    }

    private static <T extends OtherResearchProduct> T mergeORP(T original, T enrich) {
        int trust = compareTrust(original, enrich);
        final T merge = mergeResult(original, enrich);

        merge.setContactperson(unionDistinctLists(merge.getContactperson(), enrich.getContactperson(), trust));
        merge.setContactgroup(unionDistinctLists(merge.getContactgroup(), enrich.getContactgroup(), trust));
        merge.setTool(unionDistinctLists(merge.getTool(), enrich.getTool(), trust));

        return merge;
    }

    private static <T extends Software> T mergeSoftware(T original, T enrich) {
        int trust = compareTrust(original, enrich);
        final T merge = mergeResult(original, enrich);

        merge.setDocumentationUrl(unionDistinctLists(merge.getDocumentationUrl(), enrich.getDocumentationUrl(), trust));
        merge.setLicense(unionDistinctLists(merge.getLicense(), enrich.getLicense(), trust));
        merge.setCodeRepositoryUrl(chooseReference(merge.getCodeRepositoryUrl(), enrich.getCodeRepositoryUrl(), trust));
        merge.setProgrammingLanguage(chooseReference(merge.getProgrammingLanguage(), enrich.getProgrammingLanguage(), trust));

        return merge;
    }

    private static <T extends Dataset> T mergeDataset(T original, T enrich) {
        int trust = compareTrust(original, enrich);
        T merge = mergeResult(original, enrich);

        merge.setStoragedate(chooseReference(merge.getStoragedate(), enrich.getStoragedate(), trust));
        merge.setDevice(chooseReference(merge.getDevice(), enrich.getDevice(), trust));
        merge.setSize(chooseReference(merge.getSize(), enrich.getSize(), trust));
        merge.setVersion(chooseReference(merge.getVersion(), enrich.getVersion(), trust));
        merge.setLastmetadataupdate(chooseReference(merge.getLastmetadataupdate(), enrich.getLastmetadataupdate(), trust));
        merge.setMetadataversionnumber(chooseReference(merge.getMetadataversionnumber(), enrich.getMetadataversionnumber(), trust));
        merge.setGeolocation(unionDistinctLists(merge.getGeolocation(), enrich.getGeolocation(), trust));

        return merge;
    }

    public static <T extends Publication> T mergePublication(T original, T enrich) {
        final int trust = compareTrust(original, enrich);
        T merged = mergeResult(original, enrich);

        merged.setJournal(chooseReference(merged.getJournal(), enrich.getJournal(), trust));

        return merged;
    }

    private static <T extends Organization> T mergeOrganization(T left, T enrich) {
        int trust = compareTrust(left, enrich);
        T merged = mergeOafEntityFields(left, enrich, trust);

        merged.setLegalshortname(chooseReference(merged.getLegalshortname(), enrich.getLegalshortname(), trust));
        merged.setLegalname(chooseReference(merged.getLegalname(), enrich.getLegalname(), trust));
        merged.setAlternativeNames(unionDistinctLists(enrich.getAlternativeNames(), merged.getAlternativeNames(), trust));
        merged.setWebsiteurl(chooseReference(merged.getWebsiteurl(), enrich.getWebsiteurl(), trust));
        merged.setLogourl(chooseReference(merged.getLogourl(), enrich.getLogourl(), trust));
        merged.setEclegalbody(chooseReference(merged.getEclegalbody(), enrich.getEclegalbody(), trust));
        merged.setEclegalperson(chooseReference(merged.getEclegalperson(), enrich.getEclegalperson(), trust));
        merged.setEcnonprofit(chooseReference(merged.getEcnonprofit(), enrich.getEcnonprofit(), trust));
        merged.setEcresearchorganization(chooseReference(merged.getEcresearchorganization(), enrich.getEcresearchorganization(), trust));
        merged.setEchighereducation(chooseReference(merged.getEchighereducation(), enrich.getEchighereducation(), trust));
        merged.setEcinternationalorganizationeurinterests(chooseReference(merged.getEcinternationalorganizationeurinterests(), enrich.getEcinternationalorganizationeurinterests(), trust));
        merged.setEcinternationalorganization(chooseReference(merged.getEcinternationalorganization(), enrich.getEcinternationalorganization(), trust));
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
        merged.setOamandatepublications(chooseReference(merged.getOamandatepublications(), enrich.getOamandatepublications(), trust));
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

        //missin in Project.merge
        merged.setTotalcost(chooseReference(merged.getTotalcost(), enrich.getTotalcost(), trust));
        merged.setFundedamount(chooseReference(merged.getFundedamount(), enrich.getFundedamount(), trust));

        // trust ??
        if (enrich.getH2020topiccode() != null && StringUtils.isEmpty(merged.getH2020topiccode())) {
            merged.setH2020topiccode(enrich.getH2020topiccode());
            merged.setH2020topicdescription(enrich.getH2020topicdescription());
        }

        merged.setH2020classification(unionDistinctLists(merged.getH2020classification(), enrich.getH2020classification(), trust));

        return merged;
    }


    /**
     * Longest lists list.
     *
     * @param a the a
     * @param b the b
     * @return the list
     */
    public static List<Field<String>> longestLists(List<Field<String>> a, List<Field<String>> b) {
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
     * @param merge the current instance
     * @param enrichment      the enrichment instance
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
        merge.setDistributionlocation(firstNonNull(merge.getDistributionlocation(), enrichment.getDistributionlocation()));
        merge.setCollectedfrom(firstNonNull(merge.getCollectedfrom(), enrichment.getCollectedfrom()));
        // pid and alternateId are used for matching
        merge.setDateofacceptance(firstNonNull(merge.getDateofacceptance(), enrichment.getDateofacceptance()));
        merge.setProcessingchargeamount(firstNonNull(merge.getProcessingchargeamount(), enrichment.getProcessingchargeamount()));
        merge.setProcessingchargecurrency(firstNonNull(merge.getProcessingchargecurrency(), enrichment.getProcessingchargecurrency()));
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