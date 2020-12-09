
package eu.dnetlib.dhp.schema.oaf.utils;

import eu.dnetlib.dhp.schema.oaf.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

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
	 * Creates an identifier from the most relevant PID (if available) in the given entity T. Returns entity.id
	 * when no PID is available
	 * @param entity the entity providing PIDs and a default ID.
	 * @param <T> the specific entity type. Currently Organization and Result subclasses are supported.
	 * @param md5 indicates whether should hash the PID value or not.
	 * @return an identifier from the most relevant PID, entity.id otherwise
	 */
	public static <T extends OafEntity> String createIdentifier(T entity, boolean md5) {
		if (Objects.isNull(entity.getPid()) || entity.getPid().isEmpty()) {
			return entity.getId();
		}

		Map<String, List<StructuredProperty>> pids = entity
			.getPid()
			.stream()
			.map(CleaningFunctions::normalizePidValue)
			.filter(IdentifierFactory::pidFilter)
			.collect(
				Collectors
					.groupingBy(
						p -> p.getQualifier().getClassid(),
						Collectors.mapping(p -> p, Collectors.toList())));

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
