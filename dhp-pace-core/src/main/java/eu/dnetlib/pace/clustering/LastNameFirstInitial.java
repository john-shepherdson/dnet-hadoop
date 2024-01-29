
package eu.dnetlib.pace.clustering;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.model.Person;

@ClusteringClass("lnfi")
public class LastNameFirstInitial extends AbstractClusteringFunction {

	private boolean DEFAULT_AGGRESSIVE = true;

	public LastNameFirstInitial(final Map<String, Object> params) {
		super(params);
	}

	@Override
	public Collection<String> apply(Config conf, List<String> fields) {
		return fields
			.stream()
			.filter(f -> !f.isEmpty())
			.map(LastNameFirstInitial::normalize)
			.map(s -> doApply(conf, s))
			.map(c -> filterBlacklisted(c, ngramBlacklist))
			.flatMap(c -> c.stream())
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toCollection(HashSet::new));
	}

	public static String normalize(final String s) {
		return fixAliases(transliterate(nfd(unicodeNormalization(s))))
			// do not compact the regexes in a single expression, would cause StackOverflowError in case of large input
			// strings
			.replaceAll("[^ \\w]+", "")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", "")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")
			.trim();
	}

	@Override
	protected Collection<String> doApply(final Config conf, final String s) {

		final List<String> res = Lists.newArrayList();

		final boolean aggressive = (Boolean) (getParams().containsKey("aggressive") ? getParams().get("aggressive")
			: DEFAULT_AGGRESSIVE);

		Person p = new Person(s, aggressive);

		if (p.isAccurate()) {
			String lastName = p.getNormalisedSurname().toLowerCase();
			String firstInitial = p.getNormalisedFirstName().toLowerCase().substring(0, 1);

			res.add(firstInitial.concat(lastName));
		} else { // is not accurate, meaning it has no defined name and surname
			List<String> fullname = Arrays.asList(p.getNormalisedFullname().split(" "));
			if (fullname.size() == 1) {
				res.add(p.getNormalisedFullname().toLowerCase());
			} else if (fullname.size() == 2) {
				res.add(fullname.get(0).substring(0, 1).concat(fullname.get(1)).toLowerCase());
				res.add(fullname.get(1).substring(0, 1).concat(fullname.get(0)).toLowerCase());
			} else {
				res.add(fullname.get(0).substring(0, 1).concat(fullname.get(fullname.size() - 1)).toLowerCase());
				res.add(fullname.get(fullname.size() - 1).substring(0, 1).concat(fullname.get(0)).toLowerCase());
			}
		}

		return res;
	}
}
