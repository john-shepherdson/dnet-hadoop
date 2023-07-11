
package eu.dnetlib.pace.tree;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.model.Field;
import eu.dnetlib.pace.model.FieldList;
import eu.dnetlib.pace.model.FieldValueImpl;
import eu.dnetlib.pace.model.Person;
import eu.dnetlib.pace.tree.support.AbstractComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("cosineSimilarity")
public class CosineSimilarity extends AbstractComparator {

	Map<String, String> params;

	public CosineSimilarity(Map<String, String> params) {
		super(params);
	}

	@Override
	public double compare(final Field a, final Field b, final Config conf) {

		if (a.isEmpty() || b.isEmpty())
			return -1;

		double[] aVector = ((FieldValueImpl) a).doubleArrayValue();
		double[] bVector = ((FieldValueImpl) b).doubleArrayValue();

		return cosineSimilarity(aVector, bVector);
	}

	double cosineSimilarity(double[] a, double[] b) {
		double dotProduct = 0;
		double normASum = 0;
		double normBSum = 0;

		for (int i = 0; i < a.length; i++) {
			dotProduct += a[i] * b[i];
			normASum += a[i] * a[i];
			normBSum += b[i] * b[i];
		}

		double eucledianDist = Math.sqrt(normASum) * Math.sqrt(normBSum);
		return dotProduct / eucledianDist;
	}

}
