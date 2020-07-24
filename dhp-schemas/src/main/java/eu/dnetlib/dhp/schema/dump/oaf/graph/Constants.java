
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Constants implements Serializable {
	// collectedFrom va con isProvidedBy -> becco da ModelSupport

	public static final String HOSTED_BY = "isHostedBy";
	public static final String HOSTS = "hosts";

	// community result uso isrelatedto

	public static final String RESULT_ENTITY = "result";
	public static final String DATASOURCE_ENTITY = "datasource";
	public static final String CONTEXT_ENTITY = "context";

	public static final String CONTEXT_ID = "60";
	public static final String CONTEXT_NS_PREFIX = "context____";

}
