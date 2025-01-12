
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("spacetrimmingfieldvalue")
public class SpaceTrimmingFieldValue extends AbstractClusteringFunction {

	public SpaceTrimmingFieldValue(final Map<String, Object> params) {
		super(params);
	}

	@Override
	protected Collection<String> doApply(final Config conf, final String s) {
		final List<String> res = Lists.newArrayList();

		res
			.add(
				StringUtils.isBlank(s) ? RandomStringUtils.random(param("randomLength"))
					: s.toLowerCase().replaceAll("\\s+", ""));

		return res;
	}

}
