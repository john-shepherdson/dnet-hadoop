
package eu.dnetlib.dhp.schema.oaf.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;

import eu.dnetlib.dhp.oa.graph.clean.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;

/**
 * Factory class for OpenAIRE identifiers in the Graph
 */
public class IdentifierFactory implements Serializable {

	public static final String ID_SEPARATOR = "::";
	public static final String ID_PREFIX_SEPARATOR = "|";
	public final static String ID_REGEX = "^[0-9][0-9]\\" + ID_PREFIX_SEPARATOR + ".{12}" + ID_SEPARATOR
		+ "[a-zA-Z0-9]{32}$";

	public final static String DOI_REGEX = "(^10\\.[0-9]{4,9}\\/[-._;()\\/:a-zA-Z0-9]+$)|" +
		"(^10\\.1002\\/[^\\s]+$)|" +
		"(^10\\.1021\\/[a-zA-Z0-9_][a-zA-Z0-9_][0-9]++$)|" +
		"(^10\\.1207\\/[a-zA-Z0-9_]+\\&[0-9]+_[0-9]+$)";

	public static final int ID_PREFIX_LEN = 12;
	public static final String NONE = "none";

	/**
	 * Creates an identifier from the most relevant PID (if available) in the given entity T. Returns entity.id
	 * when no PID is available
	 * @param entity the entity providing PIDs and a default ID.
	 * @param <T> the specific entity type. Currently Organization and Result subclasses are supported.
	 * @return an identifier from the most relevant PID, entity.id otherwise
	 */
	public static <T extends OafEntity> String createIdentifier(T entity) {

		if (Objects.isNull(entity.getPid()) || entity.getPid().isEmpty()) {
			return entity.getId();
		}

		Map<String, List<StructuredProperty>> pids = entity
				.getPid()
				.stream()
				.filter(s -> pidFilter(s))
				.collect(
						Collectors.groupingBy(p -> p.getQualifier().getClassid(),
								Collectors.mapping(p -> p, Collectors.toList()))
				);

		return pids
				.values()
				.stream()
				.flatMap(s -> s.stream())
				.min(new PidComparator<>(entity))
				.map(min -> Optional.ofNullable(pids.get(min.getQualifier().getClassid()))
						.map(p -> p.stream()
								.sorted(new PidValueComparator())
								.findFirst()
								.map(s -> idFromPid(entity, s))
								.orElseGet(entity::getId))
						.orElseGet(entity::getId))
				.orElseGet(entity::getId);
	}

	protected static boolean pidFilter(StructuredProperty s) {
		if (Objects.isNull(s.getQualifier()) ||
			StringUtils.isBlank(StringUtils.trim(s.getValue()))) {
			return false;
		}
		try {
			switch (PidType.valueOf(s.getQualifier().getClassid())) {
				case doi:
					final String doi = StringUtils.trim(StringUtils.lowerCase(s.getValue()));
					return doi.matches(DOI_REGEX);
				default:
					return true;
			}
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	private static String verifyIdSyntax(String s) {
		if (StringUtils.isBlank(s) || !s.matches(ID_REGEX)) {
			throw new RuntimeException(String.format("malformed id: '%s'", s));
		} else {
			return s;
		}
	}

	private static <T extends OafEntity> String idFromPid(T entity, StructuredProperty s) {
		return new StringBuilder()
			.append(StringUtils.substringBefore(entity.getId(), ID_PREFIX_SEPARATOR))
			.append(ID_PREFIX_SEPARATOR)
			.append(createPrefix(s.getQualifier().getClassid()))
			.append(ID_SEPARATOR)
			.append(DHPUtils.md5(CleaningFunctions.normalizePidValue(s).getValue()))
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
