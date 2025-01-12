
package eu.dnetlib.pace.tree;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Sets;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractListComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("stringListMatch")
public class StringListMatch extends AbstractListComparator {

	private static final Log log = LogFactory.getLog(StringListMatch.class);
	private Map<String, String> params;

	final private String TYPE; // percentage or count

	public StringListMatch(final Map<String, String> params) {
		super(params);
		this.params = params;

		TYPE = params.getOrDefault("type", "percentage");
	}

	@Override
	public double compare(final List<String> a, final List<String> b, final Config conf) {

		final Set<String> pa = new HashSet<>(a);
		final Set<String> pb = new HashSet<>(b);

		if (pa.isEmpty() || pb.isEmpty()) {
			return -1; // return undefined if one of the two lists is empty
		}

		int incommon = Sets.intersection(pa, pb).size();
		int simDiff = Sets.symmetricDifference(pa, pb).size();

		if (incommon + simDiff == 0) {
			return 0.0;
		}

		if (TYPE.equals("percentage"))
			return (double) incommon / (incommon + simDiff);
		else
			return incommon;

	}
}
