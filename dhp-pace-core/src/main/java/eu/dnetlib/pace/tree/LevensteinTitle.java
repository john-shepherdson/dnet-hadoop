
package eu.dnetlib.pace.tree;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("levensteinTitle")
public class LevensteinTitle extends AbstractStringComparator {

	private static final Log log = LogFactory.getLog(LevensteinTitle.class);

	public LevensteinTitle(Map<String, String> params) {
		super(params, new com.wcohen.ss.Levenstein());
	}

	public LevensteinTitle(final double w) {
		super(w, new com.wcohen.ss.Levenstein());
	}

	protected LevensteinTitle(final double w, final AbstractStringDistance ssalgo) {
		super(w, ssalgo);
	}

	@Override
	public double distance(final String ca, final String cb, final Config conf) {
		final boolean check = checkNumbers(ca, cb);

		if (check)
			return 0.5;

		Double threshold = getDoubleParam("threshold");

		// reduce Levenshtein algo complexity when target threshold is known
		if (threshold != null && threshold >= 0.0 && threshold <= 1.0) {
			int maxdistance = (int) Math.floor((1 - threshold) * Math.max(ca.length(), cb.length()));
			int score = StringUtils.getLevenshteinDistance(ca, cb, maxdistance);
			if (score == -1) {
				return 0;
			}
			return normalize(score, ca.length(), cb.length());
		} else {
			return normalize(StringUtils.getLevenshteinDistance(ca, cb), ca.length(), cb.length());
		}
	}

	private double normalize(final double score, final int la, final int lb) {
		return 1 - (Math.abs(score) / Math.max(la, lb));
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(final double d) {
		return 1 / Math.pow(Math.abs(d) + 1, 0.1);
	}

}
