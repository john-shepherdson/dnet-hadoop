
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("sortedngrampairs")
public class SortedNgramPairs extends NgramPairs {

	public SortedNgramPairs(Map<String, Object> params) {
		super(params, false);
	}

	@Override
	protected Collection<String> doApply(Config conf, String s) {

		final List<String> tokens = Lists.newArrayList(Splitter.on(" ").omitEmptyStrings().trimResults().split(s));

		Collections.sort(tokens);

		return ngramPairs(
			Lists.newArrayList(getNgrams(Joiner.on(" ").join(tokens), param("ngramLen"), param("max") * 2, 1, 2)),
			param("max"));
	}

}
