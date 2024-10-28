
package eu.dnetlib.pace.clustering;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("numAuthorsTitleSuffixPrefixChain")
public class NumAuthorsTitleSuffixPrefixChain extends AbstractClusteringFunction {

	public NumAuthorsTitleSuffixPrefixChain(Map<String, Object> params) {
		super(params);
	}

	@Override
	public Collection<String> apply(Config conf, List<String> fields) {

		try {
			int num_authors = Math.min(Integer.parseInt(fields.get(0)), 21); // SIZE threshold is 20, +1

			if (num_authors > 0) {
				return super.apply(conf, fields.subList(1, fields.size()))
					.stream()
					.map(s -> num_authors + "-" + s)
					.collect(Collectors.toList());
			}
		} catch (NumberFormatException e) {
			// missing or null authors array
		}

		return Collections.emptyList();
	}

	@Override
	protected Collection<String> doApply(Config conf, String s) {
		return suffixPrefixChain(cleanup(s), paramOrDefault("mod", 10));
	}

	private Collection<String> suffixPrefixChain(String s, int mod) {
		// create the list of words from the string (remove short words)
		List<String> wordsList = Arrays
			.stream(s.split(" "))
			.filter(si -> si.length() > 3)
			.collect(Collectors.toList());

		final int words = wordsList.size();
		final int letters = s.length();

		// create the prefix: number of words + number of letters/mod
		String prefix = words / mod + "-";

		return doSuffixPrefixChain(wordsList, prefix);

	}

	private Collection<String> doSuffixPrefixChain(List<String> wordsList, String prefix) {

		Set<String> set = Sets.newLinkedHashSet();
		switch (wordsList.size()) {
			case 0:
				break;
			case 1:
				set.add(wordsList.get(0));
				break;
			case 2:
				set
					.add(
						prefix +
							suffix(wordsList.get(0), 3) +
							prefix(wordsList.get(1), 3));

				set
					.add(
						prefix +
							prefix(wordsList.get(0), 3) +
							suffix(wordsList.get(1), 3));

				break;
			default:
				set
					.add(
						prefix +
							suffix(wordsList.get(0), 3) +
							prefix(wordsList.get(1), 3) +
							suffix(wordsList.get(2), 3));

				set
					.add(
						prefix +
							prefix(wordsList.get(0), 3) +
							suffix(wordsList.get(1), 3) +
							prefix(wordsList.get(2), 3));
				break;
		}

		return set;

	}

	private String suffix(String s, int len) {
		return s.substring(s.length() - len);
	}

	private String prefix(String s, int len) {
		return s.substring(0, len);
	}

}
