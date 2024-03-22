
package eu.dnetlib.dhp.schema.oaf.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PidBlacklistProvider {

	private static final PidBlacklist blacklist;

	static {
		try {
			String json = IOUtils.toString(IdentifierFactory.class.getResourceAsStream("pid_blacklist.json"));
			blacklist = new ObjectMapper().readValue(json, PidBlacklist.class);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static PidBlacklist getBlacklist() {
		return blacklist;
	}

	public static Set<String> getBlacklist(String pidType) {
		return Optional
			.ofNullable(getBlacklist().get(pidType))
			.orElse(new HashSet<>());
	}

	private PidBlacklistProvider() {}

}
