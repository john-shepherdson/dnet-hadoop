
package eu.dnetlib.dhp.schema.oaf;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.utils.PidBlacklistProvider;

public class CleaningFunctions {

	public static final String DOI_PREFIX_REGEX = "(^10\\.|\\/10.)";
	public static final String DOI_PREFIX = "10.";

	public static final String ORCID_PREFIX_REGEX = "^http(s?):\\/\\/orcid\\.org\\/";
	public static final String CLEANING_REGEX = "(?:\\n|\\r|\\t)";

	public static final Set<String> PID_BLACKLIST = new HashSet<>();

	static {
		PID_BLACKLIST.add("none");
		PID_BLACKLIST.add("na");
	}

	public static <T extends Oaf> T fixVocabularyNames(T value) {
		if (value instanceof Datasource) {
			// nothing to clean here
		} else if (value instanceof Project) {
			// nothing to clean here
		} else if (value instanceof Organization) {
			Organization o = (Organization) value;
			if (Objects.nonNull(o.getCountry())) {
				fixVocabName(o.getCountry(), ModelConstants.DNET_COUNTRY_TYPE);
			}
		} else if (value instanceof Relation) {
			// nothing to clean here
		} else if (value instanceof Result) {

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

			} else if (value instanceof eu.dnetlib.dhp.schema.oaf.Dataset) {

			} else if (value instanceof OtherResearchProduct) {

			} else if (value instanceof Software) {

			}
		}

		return value;
	}

	public static <T extends Oaf> T cleanup(T value) {
		if (value instanceof Datasource) {
			// nothing to clean here
		} else if (value instanceof Project) {
			// nothing to clean here
		} else if (value instanceof Organization) {
			Organization o = (Organization) value;
			if (Objects.isNull(o.getCountry()) || StringUtils.isBlank(o.getCountry().getClassid())) {
				o.setCountry(ModelConstants.UNKNOWN_COUNTRY);
			}
		} else if (value instanceof Relation) {
			// nothing to clean here
		} else if (value instanceof Result) {

			Result r = (Result) value;
			if (Objects.nonNull(r.getPublisher()) && StringUtils.isBlank(r.getPublisher().getValue())) {
				r.setPublisher(null);
			}
			if (Objects.isNull(r.getLanguage()) || StringUtils.isBlank(r.getLanguage().getClassid())) {
				r
					.setLanguage(
						qualifier("und", "Undetermined", ModelConstants.DNET_LANGUAGES));
			}
			if (Objects.nonNull(r.getSubject())) {
				r
					.setSubject(
						r
							.getSubject()
							.stream()
							.filter(Objects::nonNull)
							.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
							.filter(sp -> Objects.nonNull(sp.getQualifier()))
							.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
							.map(CleaningFunctions::cleanValue)
							.collect(Collectors.toList()));
			}
			if (Objects.nonNull(r.getTitle())) {
				r
					.setTitle(
						r
							.getTitle()
							.stream()
							.filter(Objects::nonNull)
							.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
							.map(CleaningFunctions::cleanValue)
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
							.map(CleaningFunctions::cleanValue)
							.collect(Collectors.toList()));
			}
			if (Objects.nonNull(r.getPid())) {
				r.setPid(processPidCleaning(r.getPid()));
			}
			if (Objects.isNull(r.getResourcetype()) || StringUtils.isBlank(r.getResourcetype().getClassid())) {
				r
					.setResourcetype(
						qualifier(ModelConstants.UNKNOWN, "Unknown", ModelConstants.DNET_DATA_CITE_RESOURCE));
			}
			if (Objects.nonNull(r.getInstance())) {

				for (Instance i : r.getInstance()) {
					Optional
						.ofNullable(i.getPid())
						.ifPresent(pid -> {
							final Set<StructuredProperty> pids = pid
								.stream()
								.filter(Objects::nonNull)
								.filter(p -> StringUtils.isNotBlank(p.getValue()))
								.collect(Collectors.toCollection(HashSet::new));

							Optional
								.ofNullable(i.getAlternateIdentifier())
								.ifPresent(altId -> {
									final Set<StructuredProperty> altIds = altId
										.stream()
										.filter(Objects::nonNull)
										.filter(p -> StringUtils.isNotBlank(p.getValue()))
										.collect(Collectors.toCollection(HashSet::new));

									i.setAlternateIdentifier(Lists.newArrayList(Sets.difference(altIds, pids)));
								});
						});

					if (Objects.isNull(i.getAccessright()) || StringUtils.isBlank(i.getAccessright().getClassid())) {
						i
							.setAccessright(
								accessRight(
									ModelConstants.UNKNOWN, ModelConstants.NOT_AVAILABLE,
									ModelConstants.DNET_ACCESS_MODES));
					}
					if (Objects.isNull(i.getHostedby()) || StringUtils.isBlank(i.getHostedby().getKey())) {
						i.setHostedby(ModelConstants.UNKNOWN_REPOSITORY);
					}
					if (Objects.isNull(i.getRefereed())) {
						i.setRefereed(qualifier("0000", "Unknown", ModelConstants.DNET_REVIEW_LEVELS));
					}
				}
			}
			if (Objects.isNull(r.getBestaccessright()) || StringUtils.isBlank(r.getBestaccessright().getClassid())) {
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
									.map(p -> {
										p.setValue(p.getValue().trim().replaceAll(ORCID_PREFIX_REGEX, ""));
										return p;
									})
									.filter(p -> StringUtils.isNotBlank(p.getValue()))
									.collect(
										Collectors
											.toMap(
												StructuredProperty::getValue, Function.identity(), (p1, p2) -> p1,
												LinkedHashMap::new))
									.values()
									.stream()
									.collect(Collectors.toList()));
					}
				}

			}
			if (value instanceof Publication) {

			} else if (value instanceof eu.dnetlib.dhp.schema.oaf.Dataset) {

			} else if (value instanceof OtherResearchProduct) {

			} else if (value instanceof Software) {

			}
		}

		return value;
	}

	private static List<StructuredProperty> processPidCleaning(List<StructuredProperty> pids) {
		return pids
			.stream()
			.filter(Objects::nonNull)
			.filter(sp -> StringUtils.isNotBlank(StringUtils.trim(sp.getValue())))
			.filter(sp -> !PID_BLACKLIST.contains(sp.getValue().trim().toLowerCase()))
			.filter(sp -> Objects.nonNull(sp.getQualifier()))
			.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
			.map(CleaningFunctions::normalizePidValue)
			.filter(CleaningFunctions::pidFilter)
			.collect(Collectors.toList());
	}

	protected static StructuredProperty cleanValue(StructuredProperty s) {
		s.setValue(s.getValue().replaceAll(CLEANING_REGEX, " "));
		return s;
	}

	protected static Field<String> cleanValue(Field<String> s) {
		s.setValue(s.getValue().replaceAll(CLEANING_REGEX, " "));
		return s;
	}

	// HELPERS

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

	/**
	 * Utility method that filter PID values on a per-type basis.
	 * @param s the PID whose value will be checked.
	 * @return false if the pid matches the filter criteria, true otherwise.
	 */
	public static boolean pidFilter(StructuredProperty s) {
		final String pidValue = s.getValue();
		if (Objects.isNull(s.getQualifier()) ||
			StringUtils.isBlank(pidValue) ||
			StringUtils.isBlank(pidValue.replaceAll("(?:\\n|\\r|\\t|\\s)", ""))) {
			return false;
		}
		if (CleaningFunctions.PID_BLACKLIST.contains(pidValue)) {
			return false;
		}
		if (PidBlacklistProvider.getBlacklist(s.getQualifier().getClassid()).contains(pidValue)) {
			return false;
		}
		return true;
	}

	/**
	 * Utility method that normalises PID values on a per-type basis.
	 * @param pid the PID whose value will be normalised.
	 * @return the PID containing the normalised value.
	 */
	public static StructuredProperty normalizePidValue(StructuredProperty pid) {
		String value = Optional
			.ofNullable(pid.getValue())
			.map(String::trim)
			.orElseThrow(() -> new IllegalArgumentException("PID value cannot be empty"));
		switch (pid.getQualifier().getClassid()) {

			// TODO add cleaning for more PID types as needed
			case "doi":
				pid.setValue(value.toLowerCase().replaceFirst(DOI_PREFIX_REGEX, DOI_PREFIX));
				break;
		}
		return pid;
	}

}
