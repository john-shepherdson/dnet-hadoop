
package eu.dnetlib.pace.tree;

import java.util.Map;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("numbersComparator")
public class NumbersComparator extends AbstractStringComparator {

	Map<String, String> params;

	public NumbersComparator(Map<String, String> params) {
		super(params);
		this.params = params;
	}

	@Override
	public double distance(String a, String b, Config conf) {

		// extracts numbers from the field
		String numbers1 = getNumbers(nfd(a));
		String numbers2 = getNumbers(nfd(b));

		if (numbers1.isEmpty() || numbers2.isEmpty())
			return -1.0;

		int n1 = Integer.parseInt(numbers1);
		int n2 = Integer.parseInt(numbers2);

		return Math.abs(n1 - n2);
	}
}
