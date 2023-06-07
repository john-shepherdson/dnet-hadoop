package eu.dnetlib.pace.clustering;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import eu.dnetlib.pace.config.Config;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@ClusteringClass("lowercase")
public class LowercaseClustering extends AbstractClusteringFunction {

	public LowercaseClustering(final Map<String, Integer> params) {
		super(params);
	}

	@Override
	public Collection<String> apply(Config conf, List<String> fields) {
		Collection<String> c = Sets.newLinkedHashSet();
		for(String f : fields) {
			c.addAll(doApply(conf, f));
		}
		return c;
	}

	@Override
	protected Collection<String> doApply(final Config conf, final String s) {
		if(StringUtils.isBlank(s)) {
			return Lists.newArrayList();
		}
		return Lists.newArrayList(s.toLowerCase().trim());
	}
}
