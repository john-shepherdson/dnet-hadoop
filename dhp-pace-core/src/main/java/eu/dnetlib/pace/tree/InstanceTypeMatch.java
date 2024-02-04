
package eu.dnetlib.pace.tree;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractListComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("instanceTypeMatch")
public class InstanceTypeMatch extends AbstractListComparator {

	final Map<String, String> translationMap = new HashMap<>();

	public InstanceTypeMatch(Map<String, String> params) {
		super(params);


		// chaper of book in article
		//  in article

		// jolly types
		translationMap.put("Conference object", "*");
		translationMap.put("Research", "*");
		translationMap.put("Other literature type", "*");
		translationMap.put("Unknown", "*");
		translationMap.put("UNKNOWN", "*");

		// article types
		translationMap.put("Article", "Article");
		// Book
		translationMap.put("Data Paper", "Article");
		translationMap.put("Software Paper", "Article");
		translationMap.put("Preprint", "Article");
		translationMap.put("Part of book or chapter of book", "Article");
		//Journal


		// thesis types
		translationMap.put("Thesis", "Thesis");
		translationMap.put("Master thesis", "Thesis");
		translationMap.put("Bachelor thesis", "Thesis");
		translationMap.put("Doctoral thesis", "Thesis");
	}

	@Override
	public double compare(final List<String> a, final List<String> b, final Config conf) {

		if (a == null || b == null) {
			return -1;
		}

		if (a.isEmpty() || b.isEmpty()) {
			return -1;
		}

		final Set<String> ca = a.stream().map(this::translate).collect(Collectors.toSet());
		final Set<String> cb = b.stream().map(this::translate).collect(Collectors.toSet());

		// if at least one is a jolly type, it must produce a match
		if (ca.contains("*") || cb.contains("*"))
			return 1.0;

		int incommon = Sets.intersection(ca, cb).size();

		// if at least one is in common, it must produce a match
		return incommon >= 1 ? 1 : 0;
	}

	public String translate(String term) {
		return translationMap.getOrDefault(term, term);
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(final double d) {
		return d;
	}
}
