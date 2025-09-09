
package eu.dnetlib.dhp.bulktag.resolver;

import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;

public class EntityResolverFactory {

	private EntityResolverFactory() {
	}

	public static EntityResolver newInstance() {

		return new JoinEntityResolver();
	}
}
