
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.Map;

import eu.dnetlib.pace.config.Config;

public class RandomClusteringFunction extends AbstractClusteringFunction {

	public RandomClusteringFunction(Map<String, Object> params) {
		super(params);
	}

	@Override
	protected Collection<String> doApply(final Config conf, String s) {
		return null;
	}

}
