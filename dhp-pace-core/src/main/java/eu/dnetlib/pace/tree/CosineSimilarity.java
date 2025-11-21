
package eu.dnetlib.pace.tree;

import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;
import scala.collection.convert.Wrappers;
import scala.collection.convert.Wrappers.SeqWrapper;

@ComparatorClass("cosineSimilarity")
public class CosineSimilarity extends AbstractComparator<double[]> {

	Map<String, String> params;

	public CosineSimilarity(final Map<String, String> params) {
		super(params);
	}

	@Override
	public double compare(final Object a, final Object b, final Config config) {
		if (a instanceof double[]) { return compare((double[]) a, (double[]) b, config); }

		final Wrappers.SeqWrapper<?> seqA = (SeqWrapper<?>) a;
		final Wrappers.SeqWrapper<?> seqB = (SeqWrapper<?>) b;

		final double[] arrA = ArrayUtils.toPrimitive(seqA.toArray(new Double[seqA.size()]));
		final double[] arrB = ArrayUtils.toPrimitive(seqB.toArray(new Double[seqB.size()]));

		return compare(arrA, arrB, config);
	}

	public double compare(final double[] a, final double[] b, final Config conf) {

		if ((a.length == 0) || (b.length == 0)) { return -1; }

		return cosineSimilarity(a, b);
	}

	double cosineSimilarity(final double[] a, final double[] b) {
		double dotProduct = 0;
		double normASum = 0;
		double normBSum = 0;

		for (int i = 0; i < a.length; i++) {
			dotProduct += a[i] * b[i];
			normASum += a[i] * a[i];
			normBSum += b[i] * b[i];
		}

		final double eucledianDist = Math.sqrt(normASum) * Math.sqrt(normBSum);
		return dotProduct / eucledianDist;
	}

}
