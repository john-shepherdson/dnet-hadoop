package eu.dnetlib.pace.distance;

import eu.dnetlib.pace.model.Field;

import java.util.Map;

/**
 * Each field is configured with a compare algo which knows how to compute the compare (0-1) between the fields of two
 * objects.
 */
public interface DistanceAlgo {

	public abstract double distance(Field a, Field b);

	public double getWeight();

}
