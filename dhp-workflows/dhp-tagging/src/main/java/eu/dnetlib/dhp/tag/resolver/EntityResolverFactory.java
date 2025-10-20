
package eu.dnetlib.dhp.tag.resolver;

public class EntityResolverFactory {

	private EntityResolverFactory() {
	}

	public static EntityResolver newInstance() {

		return new JoinEntityResolver();
	}
}
