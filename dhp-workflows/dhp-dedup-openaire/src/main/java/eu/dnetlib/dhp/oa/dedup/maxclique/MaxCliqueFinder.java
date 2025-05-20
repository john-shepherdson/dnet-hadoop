package eu.dnetlib.dhp.oa.dedup.maxclique;

import java.util.Collection;

/**
 * @author Steve Ash
 */
public interface MaxCliqueFinder<T> {
    Clique<T> findMaximum(Collection<T> elements, Weigher<T> weigher);
}
