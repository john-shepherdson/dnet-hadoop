
package eu.dnetlib.pace.clustering;

import java.util.*;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("ngrams")
public class Ngrams extends AbstractClusteringFunction {

	private final boolean sorted;

	public Ngrams(Map<String, Object> params) {
		this(params, false);
	}

	public Ngrams(Map<String, Object> params, boolean sorted) {
		super(params);
		this.sorted = sorted;
	}

	@Override
	protected Collection<String> doApply(Config conf, String s) {
		return getNgrams(s, param("ngramLen"), param("max"), param("maxPerToken"), param("minNgramLen"));
	}

	protected Collection<String> getNgrams(String s, int ngramLen, int max, int maxPerToken, int minNgramLen) {

		final Collection<String> ngrams = sorted ? new TreeSet<>() : new LinkedHashSet<String>();
		final StringTokenizer st = new StringTokenizer(s);

		while (st.hasMoreTokens()) {
			final String token = st.nextToken();
			if (!token.isEmpty()) {
				for (int i = 0; i < maxPerToken && ngramLen + i <= token.length(); i++) {
					String ngram = token.substring(i, Math.min(ngramLen + i, token.length())).trim();

					if (ngram.length() >= minNgramLen) {
						ngrams.add(ngram);

						if (ngrams.size() >= max) {
							return ngrams;
						}
					}
				}
			}
		}
		// System.out.println(ngrams + " n: " + ngrams.size());
		return ngrams;
	}

}
