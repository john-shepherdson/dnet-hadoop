
package eu.dnetlib.dhp.schema.oaf.utils;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.common.ModelConstants.OPENAIRE_META_RESOURCE_TYPE;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.getProvenance;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.github.sisyphsu.dateparser.DateParserUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.common.vocabulary.VocabularyTerm;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import me.xuender.unidecode.Unidecode;

public class GraphCleaningFunctions extends CleaningFunctions {

	public static final String DNET_PUBLISHERS = "dnet:publishers";

	public static final String DNET_LICENSES = "dnet:licenses";

	public static final String ORCID_CLEANING_REGEX = ".*([0-9]{4}).*[-–—−=].*([0-9]{4}).*[-–—−=].*([0-9]{4}).*[-–—−=].*([0-9x]{4})";
	public static final int ORCID_LEN = 19;
	public static final String CLEANING_REGEX = "(?:\\n|\\r|\\t)";
	public static final String INVALID_AUTHOR_REGEX = ".*deactivated.*";

	public static final String TITLE_TEST = "test";
	public static final String TITLE_FILTER_REGEX = String.format("(%s)|\\W|\\d", TITLE_TEST);

	public static final int TITLE_FILTER_RESIDUAL_LENGTH = 5;
	private static final String NAME_CLEANING_REGEX = "[\\r\\n\\t\\s]+";

	private static final Set<String> INVALID_AUTHOR_NAMES = new HashSet<>();

	private static final Set<String> INVALID_URLS = new HashSet<>();

	private static final Set<String> INVALID_URL_HOSTS = new HashSet<>();

	private static final HashSet<String> PEER_REVIEWED_TYPES = new HashSet<>();

	static {
		PEER_REVIEWED_TYPES.add("Article");
		PEER_REVIEWED_TYPES.add("Part of book or chapter of book");
		PEER_REVIEWED_TYPES.add("Book");
		PEER_REVIEWED_TYPES.add("Doctoral thesis");
		PEER_REVIEWED_TYPES.add("Master thesis");
		PEER_REVIEWED_TYPES.add("Data Paper");
		PEER_REVIEWED_TYPES.add("Thesis");
		PEER_REVIEWED_TYPES.add("Bachelor thesis");
		PEER_REVIEWED_TYPES.add("Conference object");

		INVALID_AUTHOR_NAMES.add("(:null)");
		INVALID_AUTHOR_NAMES.add("(:unap)");
		INVALID_AUTHOR_NAMES.add("(:tba)");
		INVALID_AUTHOR_NAMES.add("(:unas)");
		INVALID_AUTHOR_NAMES.add("(:unav)");
		INVALID_AUTHOR_NAMES.add("(:unkn)");
		INVALID_AUTHOR_NAMES.add("(:unkn) unknown");
		INVALID_AUTHOR_NAMES.add(":none");
		INVALID_AUTHOR_NAMES.add(":null");
		INVALID_AUTHOR_NAMES.add(":unas");
		INVALID_AUTHOR_NAMES.add(":unav");
		INVALID_AUTHOR_NAMES.add(":unkn");
		INVALID_AUTHOR_NAMES.add("[autor desconocido]");
		INVALID_AUTHOR_NAMES.add("[s. n.]");
		INVALID_AUTHOR_NAMES.add("[s.n]");
		INVALID_AUTHOR_NAMES.add("[unknown]");
		INVALID_AUTHOR_NAMES.add("anonymous");
		INVALID_AUTHOR_NAMES.add("n.n.");
		INVALID_AUTHOR_NAMES.add("nn");
		INVALID_AUTHOR_NAMES.add("no name supplied");
		INVALID_AUTHOR_NAMES.add("none");
		INVALID_AUTHOR_NAMES.add("none available");
		INVALID_AUTHOR_NAMES.add("not available not available");
		INVALID_AUTHOR_NAMES.add("null &na;");
		INVALID_AUTHOR_NAMES.add("null anonymous");
		INVALID_AUTHOR_NAMES.add("unbekannt");
		INVALID_AUTHOR_NAMES.add("unknown");
		INVALID_AUTHOR_NAMES.add("autor, Sin");
		INVALID_AUTHOR_NAMES.add("Desconocido / Inconnu,");

		INVALID_URL_HOSTS.add("creativecommons.org");
		INVALID_URL_HOSTS.add("www.academia.edu");
		INVALID_URL_HOSTS.add("academia.edu");
		INVALID_URL_HOSTS.add("researchgate.net");
		INVALID_URL_HOSTS.add("www.researchgate.net");

		INVALID_URLS.add("http://repo.scoap3.org/api");
		INVALID_URLS.add("http://ora.ox.ac.uk/objects/uuid:");
		INVALID_URLS.add("http://ntur.lib.ntu.edu.tw/news/agent_contract.pdf");
		INVALID_URLS.add("https://media.springer.com/full/springer-instructions-for-authors-assets/pdf/SN_BPF_EN.pdf");
		INVALID_URLS.add("http://www.tobaccoinduceddiseases.org/dl/61aad426c96519bea4040a374c6a6110/");
		INVALID_URLS.add("https://www.bilboard.nl/verenigingsbladen/bestuurskundige-berichten");
	}

	public static <T extends Oaf> T cleanContext(T value, String contextId, String verifyParam) {
		if (ModelSupport.isSubClass(value, Result.class)) {
			final Result res = (Result) value;
			if (shouldCleanContext(res, verifyParam)) {
				res
					.setContext(
						res
							.getContext()
							.stream()
							.filter(c -> !StringUtils.startsWith(c.getId().toLowerCase(), contextId))
							.collect(Collectors.toList()));
			}
			return (T) res;
		} else {
			return value;
		}
	}

	private static boolean shouldCleanContext(Result res, String verifyParam) {
		boolean titleMatch = res
			.getTitle()
			.stream()
			.filter(
				t -> t
					.getQualifier()
					.getClassid()
					.equalsIgnoreCase(ModelConstants.MAIN_TITLE_QUALIFIER.getClassid()))
			.anyMatch(t -> t.getValue().toLowerCase().startsWith(verifyParam.toLowerCase()));

		return titleMatch && Objects.nonNull(res.getContext());
	}

	public static <T extends Oaf> T cleanCountry(T value, String[] verifyParam, Set<String> hostedBy,
		String collectedfrom, String country) {
		if (ModelSupport.isSubClass(value, Result.class)) {
			final Result res = (Result) value;
			if (res.getInstance().stream().anyMatch(i -> hostedBy.contains(i.getHostedby().getKey())) ||
				!res.getCollectedfrom().stream().anyMatch(cf -> cf.getValue().equals(collectedfrom))) {
				return (T) res;
			}

			List<StructuredProperty> ids = getPidsAndAltIds(res).collect(Collectors.toList());
			if (ids
				.stream()
				.anyMatch(
					p -> p
						.getQualifier()
						.getClassid()
						.equals(PidType.doi.toString()) && pidInParam(p.getValue(), verifyParam))) {
				res
					.setCountry(
						res
							.getCountry()
							.stream()
							.filter(
								c -> toTakeCountry(c, country))
							.collect(Collectors.toList()));
			}

			return (T) res;
		} else {
			return value;
		}
	}

	private static <T extends Result> Stream<StructuredProperty> getPidsAndAltIds(T r) {
		final Stream<StructuredProperty> resultPids = Optional
			.ofNullable(r.getPid())
			.map(Collection::stream)
			.orElse(Stream.empty());

		final Stream<StructuredProperty> instancePids = Optional
			.ofNullable(r.getInstance())
			.map(
				instance -> instance
					.stream()
					.flatMap(
						i -> Optional
							.ofNullable(i.getPid())
							.map(Collection::stream)
							.orElse(Stream.empty())))
			.orElse(Stream.empty());

		final Stream<StructuredProperty> instanceAltIds = Optional
			.ofNullable(r.getInstance())
			.map(
				instance -> instance
					.stream()
					.flatMap(
						i -> Optional
							.ofNullable(i.getAlternateIdentifier())
							.map(Collection::stream)
							.orElse(Stream.empty())))
			.orElse(Stream.empty());

		return Stream
			.concat(
				Stream.concat(resultPids, instancePids),
				instanceAltIds);
	}

	private static boolean pidInParam(String value, String[] verifyParam) {
		for (String s : verifyParam)
			if (value.startsWith(s))
				return true;
		return false;
	}

	private static boolean toTakeCountry(Country c, String country) {
		// If dataInfo is not set, or dataInfo.inferenceprovenance is not set or not present then it cannot be
		// inserted via propagation
		if (!Optional.ofNullable(c.getDataInfo()).isPresent())
			return true;
		if (!Optional.ofNullable(c.getDataInfo().getInferenceprovenance()).isPresent())
			return true;
		return !(c
			.getClassid()
			.equalsIgnoreCase(country) &&
			c.getDataInfo().getInferenceprovenance().equals("propagation"));
	}

	public static <T extends Oaf> T fixVocabularyNames(T value) {
		if (value instanceof OafEntity) {

			OafEntity e = (OafEntity) value;

			Optional
				.ofNullable(e.getPid())
				.ifPresent(pid -> pid.forEach(p -> fixVocabName(p.getQualifier(), ModelConstants.DNET_PID_TYPES)));

			if (value instanceof Result) {
				Result r = (Result) value;

				fixVocabName(r.getLanguage(), ModelConstants.DNET_LANGUAGES);
				fixVocabName(r.getResourcetype(), ModelConstants.DNET_DATA_CITE_RESOURCE);
				fixVocabName(r.getBestaccessright(), ModelConstants.DNET_ACCESS_MODES);

				if (Objects.nonNull(r.getSubject())) {
					r.getSubject().forEach(s -> fixVocabName(s.getQualifier(), ModelConstants.DNET_SUBJECT_TYPOLOGIES));
				}
				if (Objects.nonNull(r.getInstance())) {
					for (Instance i : r.getInstance()) {
						fixVocabName(i.getAccessright(), ModelConstants.DNET_ACCESS_MODES);
						fixVocabName(i.getRefereed(), ModelConstants.DNET_REVIEW_LEVELS);
						Optional
							.ofNullable(i.getPid())
							.ifPresent(
								pid -> pid.forEach(p -> fixVocabName(p.getQualifier(), ModelConstants.DNET_PID_TYPES)));

					}
				}
				if (Objects.nonNull(r.getAuthor())) {
					r.getAuthor().stream().filter(Objects::nonNull).forEach(a -> {
						if (Objects.nonNull(a.getPid())) {
							a.getPid().stream().filter(Objects::nonNull).forEach(p -> {
								fixVocabName(p.getQualifier(), ModelConstants.DNET_PID_TYPES);
							});
						}
					});
				}
				if (value instanceof Publication) {

				} else if (value instanceof Dataset) {

				} else if (value instanceof OtherResearchProduct) {

				} else if (value instanceof Software) {

				}
			} else if (value instanceof Datasource) {
				// nothing to clean here
			} else if (value instanceof Project) {
				// nothing to clean here
			} else if (value instanceof Organization) {
				Organization o = (Organization) value;
				if (Objects.nonNull(o.getCountry())) {
					fixVocabName(o.getCountry(), ModelConstants.DNET_COUNTRY_TYPE);
				}

			}
		} else if (value instanceof Relation) {
			// nothing to clean here
		}

		return value;
	}

	public static <T extends Oaf> boolean filter(T value) {
		if (!(value instanceof Relation) && (Boolean.TRUE
			.equals(
				Optional
					.ofNullable(value)
					.map(
						o -> Optional
							.ofNullable(o.getDataInfo())
							.map(
								d -> Optional
									.ofNullable(d.getInvisible())
									.orElse(true))
							.orElse(false))
					.orElse(true)))) {
			return true;
		}

		if (value instanceof Datasource) {
			final Datasource d = (Datasource) value;
			return Objects.nonNull(d.getOfficialname()) && StringUtils.isNotBlank(d.getOfficialname().getValue());
		} else if (value instanceof Project) {
			final Project p = (Project) value;
			return Objects.nonNull(p.getCode()) && StringUtils.isNotBlank(p.getCode().getValue());
		} else if (value instanceof Organization) {
			// nothing to evaluate here
		} else if (value instanceof Relation) {
			// nothing to clean here
		} else if (value instanceof Result) {

			Result r = (Result) value;

			if (Objects.isNull(r.getTitle()) || r.getTitle().isEmpty()) {
				return false;
			}

			if (value instanceof Publication) {

			} else if (value instanceof Dataset) {

			} else if (value instanceof OtherResearchProduct) {

			} else if (value instanceof Software) {

			}
		}
		return true;
	}

	public static <T extends Oaf> T cleanup(T value, VocabularyGroup vocs) {

		if (Objects.isNull(value.getDataInfo())) {
			final DataInfo d = new DataInfo();
			d.setDeletedbyinference(false);
			value.setDataInfo(d);
		}

		if (value instanceof OafEntity) {

			OafEntity e = (OafEntity) value;
			if (Objects.nonNull(e.getPid())) {
				e.setPid(processPidCleaning(e.getPid()));
			}

			if (value instanceof Datasource) {
				// nothing to clean here
			} else if (value instanceof Project) {
				// nothing to clean here
			} else if (value instanceof Person) {
				// nothing to clean here
			} else if (value instanceof Organization) {
				Organization o = (Organization) value;
				if (Objects.isNull(o.getCountry()) || StringUtils.isBlank(o.getCountry().getClassid())) {
					o.setCountry(ModelConstants.UNKNOWN_COUNTRY);
				}
			} else if (value instanceof Result) {
				Result r = (Result) value;

				if (Objects.isNull(r.getContext())) {
					r.setContext(new ArrayList<>());
				}

				if (Objects.nonNull(r.getFulltext())
					&& (ModelConstants.SOFTWARE_RESULTTYPE_CLASSID.equals(r.getResulttype().getClassid()) ||
						ModelConstants.DATASET_RESULTTYPE_CLASSID.equals(r.getResulttype().getClassid()))) {
					r.setFulltext(null);

				}

				if (Objects.nonNull(r.getDateofacceptance())) {
					Optional<String> date = cleanDateField(r.getDateofacceptance());
					if (date.isPresent()) {
						r.getDateofacceptance().setValue(date.get());
					} else {
						r.setDateofacceptance(null);
					}
				}
				if (Objects.nonNull(r.getRelevantdate())) {
					r
						.setRelevantdate(
							r
								.getRelevantdate()
								.stream()
								.filter(Objects::nonNull)
								.filter(sp -> Objects.nonNull(sp.getQualifier()))
								.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
								.map(sp -> {
									sp.setValue(GraphCleaningFunctions.cleanDate(sp.getValue()));
									return sp;
								})
								.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
								.collect(Collectors.toList()));
				}
				if (Objects.nonNull(r.getPublisher())) {
					if (StringUtils.isBlank(r.getPublisher().getValue())) {
						r.setPublisher(null);
					} else {
						r
							.getPublisher()
							.setValue(
								r
									.getPublisher()
									.getValue()
									.replaceAll(NAME_CLEANING_REGEX, " "));

						if (vocs.vocabularyExists(DNET_PUBLISHERS)) {
							vocs
								.find(DNET_PUBLISHERS)
								.map(voc -> voc.getTermBySynonym(r.getPublisher().getValue()))
								.map(VocabularyTerm::getName)
								.ifPresent(publisher -> r.getPublisher().setValue(publisher));
						}
					}
				}
				if (Objects.isNull(r.getLanguage()) || StringUtils.isBlank(r.getLanguage().getClassid())) {
					r
						.setLanguage(
							qualifier("und", "Undetermined", ModelConstants.DNET_LANGUAGES));
				}
				if (Objects.nonNull(r.getSubject())) {
					List<Subject> subjects = Lists
						.newArrayList(
							r
								.getSubject()
								.stream()
								.filter(Objects::nonNull)
								.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
								.filter(sp -> Objects.nonNull(sp.getQualifier()))
								.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
								.map(s -> {
									if ("dnet:result_subject".equals(s.getQualifier().getClassid())) {
										s.getQualifier().setClassid(ModelConstants.DNET_SUBJECT_TYPOLOGIES);
										s.getQualifier().setClassname(ModelConstants.DNET_SUBJECT_TYPOLOGIES);
									}
									return s;
								})
								.map(GraphCleaningFunctions::cleanValue)
								.collect(
									Collectors
										.toMap(
											s -> Optional
												.ofNullable(s.getQualifier())
												.map(q -> q.getClassid() + s.getValue())
												.orElse(s.getValue()),
											Function.identity(),
											(s1, s2) -> Collections
												.min(Lists.newArrayList(s1, s2), new SubjectProvenanceComparator())))
								.values());
					r.setSubject(subjects);
				}
				if (Objects.nonNull(r.getTitle())) {
					r
						.setTitle(
							r
								.getTitle()
								.stream()
								.filter(Objects::nonNull)
								.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
								.filter(
									sp -> {
										final String title = sp
											.getValue()
											.toLowerCase();
										final String decoded = Unidecode.decode(title);

										if (StringUtils.contains(decoded, TITLE_TEST)) {
											return decoded
												.replaceAll(TITLE_FILTER_REGEX, "")
												.length() > TITLE_FILTER_RESIDUAL_LENGTH;
										}
										return !decoded
											.replaceAll("\\W|\\d", "")
											.isEmpty();
									})
								.map(GraphCleaningFunctions::cleanValue)
								.collect(Collectors.toList()));
				}
				if (Objects.nonNull(r.getFormat())) {
					r
						.setFormat(
							r
								.getFormat()
								.stream()
								.map(GraphCleaningFunctions::cleanValue)
								.collect(Collectors.toList()));
				}
				if (Objects.nonNull(r.getDescription())) {
					r
						.setDescription(
							r
								.getDescription()
								.stream()
								.filter(Objects::nonNull)
								.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
								.map(GraphCleaningFunctions::cleanValue)
								.sorted((s1, s2) -> s2.getValue().length() - s1.getValue().length())
								.limit(ModelHardLimits.MAX_ABSTRACTS)
								.collect(Collectors.toList()));
				}
				if (Objects.isNull(r.getResourcetype()) || StringUtils.isBlank(r.getResourcetype().getClassid())) {
					r
						.setResourcetype(
							qualifier(ModelConstants.UNKNOWN, "Unknown", ModelConstants.DNET_DATA_CITE_RESOURCE));
				}
				if (Objects.nonNull(r.getInstance())) {

					for (Instance i : r.getInstance()) {
						if (!vocs
							.termExists(ModelConstants.DNET_PUBLICATION_RESOURCE, i.getInstancetype().getClassid())) {
							if (r instanceof Publication) {
								i
									.setInstancetype(
										OafMapperUtils
											.qualifier(
												"0038", "Other literature type",
												ModelConstants.DNET_PUBLICATION_RESOURCE,
												ModelConstants.DNET_PUBLICATION_RESOURCE));
							} else if (r instanceof Dataset) {
								i
									.setInstancetype(
										OafMapperUtils
											.qualifier(
												"0039", "Other dataset type", ModelConstants.DNET_PUBLICATION_RESOURCE,
												ModelConstants.DNET_PUBLICATION_RESOURCE));
							} else if (r instanceof Software) {
								i
									.setInstancetype(
										OafMapperUtils
											.qualifier(
												"0040", "Other software type", ModelConstants.DNET_PUBLICATION_RESOURCE,
												ModelConstants.DNET_PUBLICATION_RESOURCE));
							} else if (r instanceof OtherResearchProduct) {
								i
									.setInstancetype(
										OafMapperUtils
											.qualifier(
												"0020", "Other ORP type", ModelConstants.DNET_PUBLICATION_RESOURCE,
												ModelConstants.DNET_PUBLICATION_RESOURCE));
							}
						}

						if (Objects.nonNull(i.getPid())) {
							i.setPid(processPidCleaning(i.getPid()));
						}
						if (Objects.nonNull(i.getAlternateIdentifier())) {
							i.setAlternateIdentifier(processPidCleaning(i.getAlternateIdentifier()));
						}
						Optional
							.ofNullable(i.getPid())
							.ifPresent(pid -> {
								final Set<HashableStructuredProperty> pids = pid
									.stream()
									.map(HashableStructuredProperty::newInstance)
									.collect(Collectors.toCollection(HashSet::new));
								Optional
									.ofNullable(i.getAlternateIdentifier())
									.ifPresent(altId -> {
										final Set<HashableStructuredProperty> altIds = altId
											.stream()
											.map(HashableStructuredProperty::newInstance)
											.collect(Collectors.toCollection(HashSet::new));
										i
											.setAlternateIdentifier(
												Sets
													.difference(altIds, pids)
													.stream()
													.map(HashableStructuredProperty::toStructuredProperty)
													.collect(Collectors.toList()));
									});
							});

						if (Objects.isNull(i.getAccessright())
							|| StringUtils.isBlank(i.getAccessright().getClassid())) {
							i
								.setAccessright(
									accessRight(
										ModelConstants.UNKNOWN, ModelConstants.NOT_AVAILABLE,
										ModelConstants.DNET_ACCESS_MODES));
						}
						if (Objects.isNull(i.getHostedby()) || StringUtils.isBlank(i.getHostedby().getKey())) {
							i.setHostedby(ModelConstants.UNKNOWN_REPOSITORY);
						}
						if (Objects.isNull(i.getRefereed()) || StringUtils.isBlank(i.getRefereed().getClassid())) {
							i.setRefereed(qualifier("0000", "Unknown", ModelConstants.DNET_REVIEW_LEVELS));
						}

						if (Objects.nonNull(i.getLicense()) && Objects.nonNull(i.getLicense().getValue())) {
							vocs
								.find(DNET_LICENSES)
								.map(voc -> voc.getTermBySynonym(i.getLicense().getValue()))
								.map(VocabularyTerm::getId)
								.ifPresent(license -> i.getLicense().setValue(license));
						}

						// from the script from Dimitris
						if ("0000".equals(i.getRefereed().getClassid())) {
							final boolean isFromCrossref = Optional
								.ofNullable(i.getCollectedfrom())
								.map(KeyValue::getKey)
								.map(id -> id.equals(ModelConstants.CROSSREF_ID))
								.orElse(false);
							final boolean hasDoi = Optional
								.ofNullable(i.getPid())
								.map(
									pid -> pid
										.stream()
										.anyMatch(
											p -> PidType.doi.toString().equals(p.getQualifier().getClassid())))
								.orElse(false);
							final boolean isPeerReviewedType = PEER_REVIEWED_TYPES
								.contains(i.getInstancetype().getClassname());
							final boolean noOtherLitType = r
								.getInstance()
								.stream()
								.noneMatch(ii -> "Other literature type".equals(ii.getInstancetype().getClassname()));
							if (isFromCrossref && hasDoi && isPeerReviewedType && noOtherLitType) {
								i.setRefereed(qualifier("0001", "peerReviewed", ModelConstants.DNET_REVIEW_LEVELS));
							} else {
								i.setRefereed(qualifier("0002", "nonPeerReviewed", ModelConstants.DNET_REVIEW_LEVELS));
							}
						}

						if (Objects.nonNull(i.getDateofacceptance())) {
							Optional<String> date = cleanDateField(i.getDateofacceptance());
							if (date.isPresent()) {
								i.getDateofacceptance().setValue(date.get());
							} else {
								i.setDateofacceptance(null);
							}
						}
						if (StringUtils.isNotBlank(i.getFulltext()) &&
							(ModelConstants.SOFTWARE_RESULTTYPE_CLASSID.equals(r.getResulttype().getClassid()) ||
								ModelConstants.DATASET_RESULTTYPE_CLASSID.equals(r.getResulttype().getClassid()))) {
							i.setFulltext(null);
						}
						if (Objects.nonNull(i.getUrl())) {
							i
								.setUrl(
									i
										.getUrl()
										.stream()
										.filter(GraphCleaningFunctions::urlFilter)
										.collect(Collectors.toList()));
						}
					}
				}
				if (Objects.isNull(r.getBestaccessright())
					|| StringUtils.isBlank(r.getBestaccessright().getClassid())) {
					Qualifier bestaccessrights = OafMapperUtils.createBestAccessRights(r.getInstance());
					if (Objects.isNull(bestaccessrights)) {
						r
							.setBestaccessright(
								qualifier(
									ModelConstants.UNKNOWN, ModelConstants.NOT_AVAILABLE,
									ModelConstants.DNET_ACCESS_MODES));
					} else {
						r.setBestaccessright(bestaccessrights);
					}
				}
				if (Objects.nonNull(r.getAuthor())) {
					r
						.setAuthor(
							r
								.getAuthor()
								.stream()
								.filter(Objects::nonNull)
								.filter(GraphCleaningFunctions::isValidAuthorName)
								.map(GraphCleaningFunctions::cleanupAuthor)
								.collect(Collectors.toList()));

					boolean nullRank = r
						.getAuthor()
						.stream()
						.anyMatch(a -> Objects.isNull(a.getRank()));
					if (nullRank) {
						int i = 1;
						for (Author author : r.getAuthor()) {
							author.setRank(i++);
						}
					}

					for (Author a : r.getAuthor()) {
						if (Objects.isNull(a.getPid())) {
							a.setPid(Lists.newArrayList());
						} else {
							a
								.setPid(
									a
										.getPid()
										.stream()
										.filter(Objects::nonNull)
										.filter(p -> Objects.nonNull(p.getQualifier()))
										.filter(p -> StringUtils.isNotBlank(p.getValue()))
										.filter(
											p -> StringUtils
												.contains(StringUtils.lowerCase(p.getQualifier().getClassid()), ORCID))
										.map(p -> {
											// hack to distinguish orcid from orcid_pending
											String pidProvenance = getProvenance(p.getDataInfo());
											if (p
												.getQualifier()
												.getClassid()
												.toLowerCase()
												.contains(ModelConstants.ORCID)) {
												if (pidProvenance
													.equals(ModelConstants.SYSIMPORT_CROSSWALK_ENTITYREGISTRY) ||
													pidProvenance.equals("ORCID_ENRICHMENT")) {
													p.getQualifier().setClassid(ModelConstants.ORCID);
												} else {
													p.getQualifier().setClassid(ModelConstants.ORCID_PENDING);
												}
												final String orcid = p
													.getValue()
													.trim()
													.toLowerCase()
													.replaceAll(ORCID_CLEANING_REGEX, "$1-$2-$3-$4");
												if (orcid.length() == ORCID_LEN) {
													p.setValue(orcid);
												} else {
													p.setValue("");
												}
											}
											return p;
										})
										.filter(p -> StringUtils.isNotBlank(p.getValue()))
										.collect(
											Collectors
												.toMap(
													p -> p.getQualifier().getClassid() + p.getValue(),
													Function.identity(),
													(p1, p2) -> p1,
													LinkedHashMap::new))
										.values()
										.stream()
										.collect(Collectors.toList()));
						}
					}
				}
				if (value instanceof Publication) {

				} else if (value instanceof Dataset) {

				} else if (value instanceof OtherResearchProduct) {

				} else if (value instanceof Software) {

				}

			}

		} else if (value instanceof Relation) {
			Relation r = (Relation) value;

			Optional<String> validationDate = doCleanDate(r.getValidationDate());
			if (validationDate.isPresent()) {
				r.setValidationDate(validationDate.get());
				r.setValidated(true);
			} else {
				r.setValidationDate(null);
				r.setValidated(false);
			}
		}

		return value;
	}

	private static Author cleanupAuthor(Author author) {
		if (StringUtils.isNotBlank(author.getFullname())) {
			author
				.setFullname(
					author
						.getFullname()
						.replaceAll(NAME_CLEANING_REGEX, " ")
						.replace("\"", "\\\""));
		}
		if (StringUtils.isNotBlank(author.getName())) {
			author
				.setName(
					author
						.getName()
						.replaceAll(NAME_CLEANING_REGEX, " ")
						.replace("\"", "\\\""));
		}
		if (StringUtils.isNotBlank(author.getSurname())) {
			author
				.setSurname(
					author
						.getSurname()
						.replaceAll(NAME_CLEANING_REGEX, " ")
						.replace("\"", "\\\""));
		}

		return author;
	}

	private static Optional<String> cleanDateField(Field<String> dateofacceptance) {
		return Optional
			.ofNullable(dateofacceptance)
			.map(Field::getValue)
			.map(GraphCleaningFunctions::cleanDate)
			.filter(Objects::nonNull);
	}

	protected static Optional<String> doCleanDate(String date) {
		return Optional.ofNullable(cleanDate(date));
	}

	public static String cleanDate(final String inputDate) {

		if (StringUtils.isBlank(inputDate)) {
			return null;
		}

		try {
			final LocalDate date = DateParserUtils
				.parseDate(inputDate.trim())
				.toInstant()
				.atZone(ZoneId.systemDefault())
				.toLocalDate();
			return DateTimeFormatter.ofPattern(ModelSupport.DATE_FORMAT).format(date);
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	// HELPERS

	private static boolean isValidAuthorName(Author a) {
		return StringUtils.isNotBlank(a.getFullname()) &&
			StringUtils.isNotBlank(a.getFullname().replaceAll("[\\W]", "")) &&
			!INVALID_AUTHOR_NAMES.contains(StringUtils.lowerCase(a.getFullname()).trim()) &&
			!Stream
				.of(a.getFullname(), a.getName(), a.getSurname())
				.filter(StringUtils::isNotBlank)
				.collect(Collectors.joining(""))
				.toLowerCase()
				.matches(INVALID_AUTHOR_REGEX);
	}

	private static boolean urlFilter(String u) {
		try {
			final URL url = new URL(u);
			if (StringUtils.isBlank(url.getPath()) || "/".equals(url.getPath())) {
				return false;
			}
			if (INVALID_URL_HOSTS.contains(url.getHost())) {
				return false;
			}
			return !INVALID_URLS.contains(url.toString());
		} catch (MalformedURLException ex) {
			return false;
		}
	}

	private static List<StructuredProperty> processPidCleaning(List<StructuredProperty> pids) {
		return pids
			.stream()
			.filter(Objects::nonNull)
			.filter(sp -> StringUtils.isNotBlank(StringUtils.trim(sp.getValue())))
			.filter(sp -> !PID_BLACKLIST.contains(sp.getValue().trim().toLowerCase()))
			.filter(sp -> Objects.nonNull(sp.getQualifier()))
			.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
			.map(PidCleaner::normalizePidValue)
			.filter(CleaningFunctions::pidFilter)
			.collect(Collectors.toList());
	}

	private static void fixVocabName(Qualifier q, String vocabularyName) {
		if (Objects.nonNull(q) && StringUtils.isBlank(q.getSchemeid())) {
			q.setSchemeid(vocabularyName);
			q.setSchemename(vocabularyName);
		}
	}

	private static AccessRight accessRight(String classid, String classname, String scheme) {
		return OafMapperUtils
			.accessRight(
				classid, classname, scheme, scheme);
	}

	private static Qualifier qualifier(String classid, String classname, String scheme) {
		return OafMapperUtils
			.qualifier(
				classid, classname, scheme, scheme);
	}

	protected static StructuredProperty cleanValue(StructuredProperty s) {
		s.setValue(s.getValue().replaceAll(CLEANING_REGEX, " "));
		return s;
	}

	protected static Subject cleanValue(Subject s) {
		s.setValue(s.getValue().replaceAll(CLEANING_REGEX, " "));
		return s;
	}

	protected static Field<String> cleanValue(Field<String> s) {
		s.setValue(s.getValue().replaceAll(CLEANING_REGEX, " "));
		return s;
	}

	public static OafEntity applyCoarVocabularies(OafEntity entity, VocabularyGroup vocs) {

		if (entity instanceof Result) {
			final Result result = (Result) entity;

			Optional
				.ofNullable(result.getInstance())
				.ifPresent(
					instances -> instances
						.forEach(
							instance -> {
								if (Objects.isNull(instance.getInstanceTypeMapping())) {
									List<InstanceTypeMapping> mapping = Lists.newArrayList();
									mapping
										.add(
											OafMapperUtils
												.instanceTypeMapping(
													instance.getInstancetype().getClassname(),
													OPENAIRE_COAR_RESOURCE_TYPES_3_1));
									instance.setInstanceTypeMapping(mapping);
								}
								Optional<InstanceTypeMapping> optionalItm = instance
									.getInstanceTypeMapping()
									.stream()
									.filter(GraphCleaningFunctions::originalResourceType)
									.findFirst();
								if (optionalItm.isPresent()) {
									InstanceTypeMapping coarItm = optionalItm.get();
									Optional
										.ofNullable(
											vocs
												.lookupTermBySynonym(
													OPENAIRE_COAR_RESOURCE_TYPES_3_1, coarItm.getOriginalType()))
										.ifPresent(type -> {
											coarItm.setTypeCode(type.getClassid());
											coarItm.setTypeLabel(type.getClassname());
										});
									final List<InstanceTypeMapping> mappings = Lists.newArrayList();
									if (vocs.vocabularyExists(OPENAIRE_USER_RESOURCE_TYPES)) {
										Optional
											.ofNullable(
												vocs
													.lookupTermBySynonym(
														OPENAIRE_USER_RESOURCE_TYPES, coarItm.getTypeCode()))
											.ifPresent(
												type -> mappings
													.add(
														OafMapperUtils
															.instanceTypeMapping(coarItm.getTypeCode(), type)));
									}
									if (!mappings.isEmpty()) {
										instance.getInstanceTypeMapping().addAll(mappings);
									}
								}
							}));
			result.setMetaResourceType(getMetaResourceType(result.getInstance(), vocs));
		}

		return entity;
	}

	private static boolean originalResourceType(InstanceTypeMapping itm) {
		return StringUtils.isNotBlank(itm.getOriginalType()) &&
			OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(itm.getVocabularyName()) &&
			StringUtils.isBlank(itm.getTypeCode()) &&
			StringUtils.isBlank(itm.getTypeLabel());
	}

	private static Qualifier getMetaResourceType(final List<Instance> instances, final VocabularyGroup vocs) {
		return Optional
			.ofNullable(instances)
			.map(ii -> {
				if (vocs.vocabularyExists(OPENAIRE_META_RESOURCE_TYPE)) {
					Optional<InstanceTypeMapping> itm = ii
						.stream()
						.filter(Objects::nonNull)
						.flatMap(
							i -> Optional
								.ofNullable(i.getInstanceTypeMapping())
								.map(Collection::stream)
								.orElse(Stream.empty()))
						.filter(t -> OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(t.getVocabularyName()))
						.findFirst();

					if (!itm.isPresent() || Objects.isNull(itm.get().getTypeCode())) {
						return null;
					} else {
						final String typeCode = itm.get().getTypeCode();
						return Optional
							.ofNullable(vocs.lookupTermBySynonym(OPENAIRE_META_RESOURCE_TYPE, typeCode))
							.orElseThrow(
								() -> new IllegalStateException("unable to find a synonym for '" + typeCode + "' in " +
									OPENAIRE_META_RESOURCE_TYPE));
					}
				} else {
					throw new IllegalStateException("vocabulary '" + OPENAIRE_META_RESOURCE_TYPE + "' not available");
				}
			})
			.orElse(null);
	}

}
