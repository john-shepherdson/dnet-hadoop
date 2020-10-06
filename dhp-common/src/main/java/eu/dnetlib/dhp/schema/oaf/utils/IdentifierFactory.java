
package eu.dnetlib.dhp.schema.oaf.utils;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory class for OpenAIRE identifiers in the Graph
 */
public class IdentifierFactory implements Serializable {

	public static final String ID_SEPARATOR = "::";
	public static final String ID_PREFIX_SEPARATOR = "|";
	public final static String ID_REGEX = "^[0-9][0-9]\\"+ID_PREFIX_SEPARATOR+".{12}"+ID_SEPARATOR+"[a-zA-Z0-9]{32}$";
	public static final int ID_PREFIX_LEN = 12;

	public static <T extends OafEntity> String createIdentifier(T entity) {

		if (Objects.isNull(entity.getPid()) || entity.getPid().isEmpty()) {
			return entity.getId();
		}

		return entity
				.getPid()
				.stream()
				.filter(s -> Objects.nonNull(s.getQualifier()))
				.filter(s -> PidType.isValid(s.getQualifier().getClassid()))
				.min(new PidComparator<>(entity))
				.map(s -> idFromPid(entity, s))
				.map(IdentifierFactory::verifyIdSyntax)
				.orElseGet(entity::getId);
	}

	private static String verifyIdSyntax(String s) {
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
		//TODO more aggressive cleaning? keep only alphanum and punctuation?
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
