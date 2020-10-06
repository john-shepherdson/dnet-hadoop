
package eu.dnetlib.dhp.schema.oaf.utils;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Factory class for OpenAIRE identifiers in the Graph
 */
public class IdentifierFactory implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(IdentifierFactory.class);

	public static final String ID_SEPARATOR = "::";
	public static final String ID_PREFIX_SEPARATOR = "|";
	public final static String ID_REGEX = "^[0-9][0-9]\\"+ID_PREFIX_SEPARATOR+".{12}"+ID_SEPARATOR+"[a-zA-Z0-9]{32}$";
	public static final int ID_PREFIX_LEN = 12;

	public static Set<String> acceptedPidTypes = new HashSet<>();

	static {
		acceptedPidTypes.add("doi");
		acceptedPidTypes.add("doi");
		acceptedPidTypes.add("doi");
		acceptedPidTypes.add("doi");
		acceptedPidTypes.add("doi");
		acceptedPidTypes.add("doi");

	}

	public static <T extends OafEntity> String createIdentifier(T entity) {

		if (Objects.isNull(entity.getPid()) || entity.getPid().isEmpty()) {
			return entity.getId();
		}

		return entity
				.getPid()
				.stream()
				.filter(s -> Objects.nonNull(s.getQualifier()))
				.filter(s -> acceptedPidTypes.contains(s.getQualifier().getClassid()))
				.max(new PidComparator<T>(entity))
				.map(s -> idFromPid(entity, s))
				.map(IdentifierFactory::verifyIdSyntax)
				.orElseGet(entity::getId);
	}

	protected static String verifyIdSyntax(String s) {
		if(StringUtils.isBlank(s) || !s.matches(ID_REGEX)) {
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
				.append(DHPUtils.md5(normalizePidValue(s.getValue())))
				.toString();
	}

	private static String normalizePidValue(String value) {
		//TODO more aggressive cleaning? keep only alphanum and punctation?
		return value.toLowerCase().replaceAll(" ", "");
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
