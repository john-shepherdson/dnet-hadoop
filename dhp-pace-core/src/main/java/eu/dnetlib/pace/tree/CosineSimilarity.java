package eu.dnetlib.pace.tree;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

import java.util.Map;

@ComparatorClass("cosineSimilarity")
public class CosineSimilarity extends AbstractComparator<double[]> {

    Map<String, String> params;

    public CosineSimilarity(Map<String,String> params) {
        super(params);
    }

    @Override
    public double compare(Object a, Object b, Config config) {
        return compare((double[])a, (double[])b, config);
    }

    public double compare(final double[] a, final double[] b, final Config conf) {

        if (a.length == 0 || b.length == 0)
            return -1;

        return cosineSimilarity(a, b);
    }

    double cosineSimilarity(double[] a, double[] b) {
        double dotProduct = 0;
        double normASum = 0;
        double normBSum = 0;

        for(int i = 0; i < a.length; i ++) {
            dotProduct += a[i] * b[i];
            normASum += a[i] * a[i];
            normBSum += b[i] * b[i];
        }

        double eucledianDist = Math.sqrt(normASum) * Math.sqrt(normBSum);
        return dotProduct / eucledianDist;
    }


}
