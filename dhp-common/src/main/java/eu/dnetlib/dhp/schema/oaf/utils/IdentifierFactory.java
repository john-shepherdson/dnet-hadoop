
package eu.dnetlib.dhp.schema.oaf.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.DHPUtils;

/**
 * Factory class for OpenAIRE identifiers in the Graph
 */
public class IdentifierFactory implements Serializable {

	public static final String ID_SEPARATOR = "::";
	public static final String ID_PREFIX_SEPARATOR = "|";

	public final static String DOI_REGEX = "(^10\\.[0-9]{4,9}\\/[-._;()\\/:a-zA-Z0-9]+$)|" +
		"(^10\\.1002\\/[^\\s]+$)|" +
		"(^10\\.1021\\/[a-zA-Z0-9_][a-zA-Z0-9_][0-9]++$)|" +
		"(^10\\.1207\\/[a-zA-Z0-9_]+\\&[0-9]+_[0-9]+$)";

	public static final int ID_PREFIX_LEN = 12;

	/**
	 * Declares the associations PID_TYPE -> [DATASOURCE ID, NAME] considered authoritative for that PID_TYPE
	 */
	public static final Map<PidType, HashBiMap<String, String>> PID_AUTHORITY = Maps.newHashMap();

	static {
		PID_AUTHORITY.put(PidType.doi, HashBiMap.create());
		PID_AUTHORITY.get(PidType.doi).put(CROSSREF_ID, "Crossref");
		PID_AUTHORITY.get(PidType.doi).put(DATACITE_ID, "Datacite");

		PID_AUTHORITY.put(PidType.pmc, HashBiMap.create());
		PID_AUTHORITY.get(PidType.pmc).put(EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central");
		PID_AUTHORITY.get(PidType.pmc).put(PUBMED_CENTRAL_ID, "PubMed Central");

		PID_AUTHORITY.put(PidType.pmid, HashBiMap.create());
		PID_AUTHORITY.get(PidType.pmid).put(EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central");
		PID_AUTHORITY.get(PidType.pmid).put(PUBMED_CENTRAL_ID, "PubMed Central");

		PID_AUTHORITY.put(PidType.arXiv, HashBiMap.create());
		PID_AUTHORITY.get(PidType.arXiv).put(ARXIV_ID, "arXiv.org e-Print Archive");
	}

	public static List<StructuredProperty> getPids(List<StructuredProperty> pid, KeyValue collectedFrom) {
		return pidFromInstance(pid, collectedFrom).distinct().collect(Collectors.toList());
	}

	/**
	 * Creates an identifier from the most relevant PID (if available) provided by a known PID authority in the given
	 * entity T. Returns entity.id when none of the PIDs meet the selection criteria is available.
	 *
	 * @param entity the entity providing PIDs and a default ID.
	 * @param <T> the specific entity type. Currently Organization and Result subclasses are supported.
	 * @param md5 indicates whether should hash the PID value or not.
	 * @return an identifier from the most relevant PID, entity.id otherwise
	 */
	public static <T extends OafEntity> String createIdentifier(T entity, boolean md5) {

		checkArgument(StringUtils.isNoneBlank(entity.getId()), "missing entity identifier");

		final Map<String, List<StructuredProperty>> pids = extractPids(entity);

		return pids
			.values()
			.stream()
			.flatMap(s -> s.stream())
			.min(new PidComparator<>(entity))
			.map(
				min -> Optional
					.ofNullable(pids.get(min.getQualifier().getClassid()))
					.map(
						p -> p
							.stream()
							.sorted(new PidValueComparator())
							.findFirst()
							.map(s -> idFromPid(entity, s, md5))
							.orElseGet(entity::getId))
					.orElseGet(entity::getId))
			.orElseGet(entity::getId);
	}

	private static <T extends OafEntity> Map<String, List<StructuredProperty>> extractPids(T entity) {
		if (entity instanceof Result) {
			return Optional
				.ofNullable(((Result) entity).getInstance())
				.map(
					instance -> mapPids(instance))
				.orElse(new HashMap<>());
		} else {
			return entity
				.getPid()
				.stream()
				.map(CleaningFunctions::normalizePidValue)
				.filter(IdentifierFactory::pidFilter)
				.collect(
					Collectors
						.groupingBy(
							p -> p.getQualifier().getClassid(),
							Collectors.mapping(p -> p, Collectors.toList())));
		}
	}

	private static Map<String, List<StructuredProperty>> mapPids(List<Instance> instance) {
		return instance
			.stream()
			.map(i -> pidFromInstance(i.getPid(), i.getCollectedfrom()))
			.flatMap(Function.identity())
			.collect(
				Collectors
					.groupingBy(
						p -> p.getQualifier().getClassid(),
						Collectors.mapping(p -> p, Collectors.toList())));
	}

	private static Stream<StructuredProperty> pidFromInstance(List<StructuredProperty> pid, KeyValue collectedFrom) {
		return Optional
			.ofNullable(pid)
			.map(
				pp -> pp
					.stream()
					// filter away PIDs provided by a DS that is not considered an authority for the
					// given PID Type
					.filter(p -> {
						final PidType pType = PidType.tryValueOf(p.getQualifier().getClassid());
						return Optional.ofNullable(collectedFrom).isPresent() &&
							Optional
								.ofNullable(PID_AUTHORITY.get(pType))
								.map(authorities -> {
									return authorities.containsKey(collectedFrom.getKey())
										|| authorities.containsValue(collectedFrom.getValue());
								})
								.orElse(false);
					})
					.map(CleaningFunctions::normalizePidValue)
					.filter(IdentifierFactory::pidFilter))
			.orElse(Stream.empty());
	}

	/**
	 * @see {@link IdentifierFactory#createIdentifier(OafEntity, boolean)}
	 */
	public static <T extends OafEntity> String createIdentifier(T entity) {

		return createIdentifier(entity, true);
	}

	protected static boolean pidFilter(StructuredProperty s) {
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
		switch (PidType.tryValueOf(s.getQualifier().getClassid())) {
			case doi:
				final String doi = StringUtils.trim(StringUtils.lowerCase(pidValue));
				return doi.matches(DOI_REGEX);
			case original:
				return false;
			default:
				return true;
		}
	}

	private static <T extends OafEntity> String idFromPid(T entity, StructuredProperty s, boolean md5) {
		return new StringBuilder()
			.append(StringUtils.substringBefore(entity.getId(), ID_PREFIX_SEPARATOR))
			.append(ID_PREFIX_SEPARATOR)
			.append(createPrefix(s.getQualifier().getClassid()))
			.append(ID_SEPARATOR)
			.append(md5 ? DHPUtils.md5(s.getValue()) : s.getValue())
			.toString();
	}

	// create the prefix (length = 12)
	private static String createPrefix(String pidType) {
		StringBuilder prefix = new StringBuilder(StringUtils.left(pidType, ID_PREFIX_LEN));
		while (prefix.length() < ID_PREFIX_LEN) {
			prefix.append("_");
		}
		return prefix.substring(0, ID_PREFIX_LEN);
	}

}
