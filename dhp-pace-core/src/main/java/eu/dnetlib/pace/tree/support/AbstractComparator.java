
package eu.dnetlib.pace.tree.support;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.config.Config;

public abstract class AbstractComparator<T> extends AbstractPaceFunctions implements Comparator<T> {

	/** The ssalgo. */
	protected AbstractStringDistance ssalgo;

	/** The weight. */
	protected double weight = 0.0;

	private Map<String, String> params;

	protected AbstractComparator(Map<String, String> params) {
		this.params = params;
	}

	protected AbstractComparator(Map<String, String> params, final AbstractStringDistance ssalgo) {
		this.params = params;
		this.weight = 1.0;
		this.ssalgo = ssalgo;
	}

	/**
	 * Instantiates a new second string compare algo.
	 *
	 * @param weight
	 *            the weight
	 * @param ssalgo
	 *            the ssalgo
	 */
	protected AbstractComparator(final double weight, final AbstractStringDistance ssalgo) {
		this.ssalgo = ssalgo;
		this.weight = weight;
	}

	protected AbstractComparator(final AbstractStringDistance ssalgo) {
		this.ssalgo = ssalgo;
	}

	/**
	 * Normalize.
	 *
	 * @param d
	 *            the d
	 * @return the double
	 */
	protected double normalize(double d) {
		return d;
	}

	/**
	 * Distance.
	 *
	 * @param a
	 *            the a
	 * @param b
	 *            the b
	 * @return the double
	 */

	protected double distance(final String a, final String b, final Config conf) {
		if (a.isEmpty() || b.isEmpty()) {
			return -1; // return -1 if a field is missing
		}
		double score = ssalgo.score(a, b);
		return normalize(score);
	}

	protected double compare(final String a, final String b, final Config conf) {
		if (a.isEmpty() || b.isEmpty())
			return -1;
		return distance(a, b, conf);
	}

	/**
	 * Convert the given argument to a List of Strings
	 *
	 * @param object
	 *            function argument
	 * @return the list
	 */
	protected List<String> toList(final Object object) {
		if (object instanceof List) {
			return (List<String>) object;
		}

		return Lists.newArrayList(object.toString());
	}

	/**
	 * Convert the given argument to a String
	 *
	 * @param object
	 *            function argument
	 * @return the list
	 */
	protected String toString(final Object object) {
		if (object instanceof List) {
			List<String> l = (List<String>) object;
			return Joiner.on(" ").join(l);
		}

		return object.toString();
	}

	protected String toFirstString(final Object object) {
		if (object instanceof List) {
			List<String> l = (List<String>) object;
			return l.isEmpty() ? "" : l.get(0);
		}

		return object.toString();
	}

	public double getWeight() {
		return this.weight;
	}

}
