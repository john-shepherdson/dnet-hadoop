
package eu.dnetlib.dhp.bulktag.criteria;

public class VerbResolverFactory {

	private VerbResolverFactory() {
	}

	public static VerbResolver newInstance() {

		return new VerbResolver();
	}
}
