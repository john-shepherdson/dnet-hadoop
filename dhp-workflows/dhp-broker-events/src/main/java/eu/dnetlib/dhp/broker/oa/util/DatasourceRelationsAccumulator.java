
package eu.dnetlib.dhp.broker.oa.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple3;

public class DatasourceRelationsAccumulator implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 3256220670651218957L;

	private List<Tuple3<String, String, String>> rels = new ArrayList<>();

	public List<Tuple3<String, String, String>> getRels() {
		return rels;
	}

	public void setRels(final List<Tuple3<String, String, String>> rels) {
		this.rels = rels;
	}

	protected void addTuple(final Tuple3<String, String, String> t) {
		rels.add(t);
	}

	public static final DatasourceRelationsAccumulator calculateTuples(final Result r) {

		final Set<String> collectedFromSet = r
			.getCollectedfrom()
			.stream()
			.map(kv -> kv.getKey())
			.filter(StringUtils::isNotBlank)
			.distinct()
			.collect(Collectors.toSet());

		final Set<String> hostedBySet = r
			.getInstance()
			.stream()
			.map(i -> i.getHostedby())
			.filter(Objects::nonNull)
			.filter(kv -> !StringUtils.equalsIgnoreCase(kv.getValue(), "Unknown Repository"))
			.map(kv -> kv.getKey())
			.filter(StringUtils::isNotBlank)
			.distinct()
			.filter(id -> !collectedFromSet.contains(id))
			.collect(Collectors.toSet());

		final DatasourceRelationsAccumulator res = new DatasourceRelationsAccumulator();
		collectedFromSet
			.stream()
			.map(
				s -> new Tuple3<>(ConversionUtils.cleanOpenaireId(r.getId()), ConversionUtils.cleanOpenaireId(s),
					BrokerConstants.COLLECTED_FROM_REL))
			.forEach(res::addTuple);

		hostedBySet
			.stream()
			.map(
				s -> new Tuple3<>(ConversionUtils.cleanOpenaireId(r.getId()), ConversionUtils.cleanOpenaireId(s),
					BrokerConstants.HOSTED_BY_REL))
			.forEach(res::addTuple);

		return res;
	}

}
