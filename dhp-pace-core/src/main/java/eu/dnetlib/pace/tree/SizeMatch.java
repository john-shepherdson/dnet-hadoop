package eu.dnetlib.pace.tree;

import com.google.common.collect.Lists;
import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractListComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

import java.util.List;
import java.util.Map;

/**
 * Returns true if the number of values in the fields is the same.
 *
 * @author claudio
 */
@ComparatorClass("sizeMatch")
public class SizeMatch extends AbstractListComparator {

    /**
     * Instantiates a new size match.
     *
     * @param params
     *            the parameters
     */
    public SizeMatch(final Map<String, String> params) {
        super(params);
    }

    @Override
    public double compare(final List<String> a, final List<String> b, final Config conf) {

        if (a.isEmpty() || b.isEmpty())
            return -1.0;

        return a.size() == b.size() ? 1.0 : 0.0;
    }

}
