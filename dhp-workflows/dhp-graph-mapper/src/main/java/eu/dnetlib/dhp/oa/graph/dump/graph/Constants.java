
package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.Serializable;

public class Constants implements Serializable {
	// collectedFrom va con isProvidedBy -> becco da ModelSupport

	public static final String IS_HOSTED_BY = "isHostedBy";
	public static final String HOSTS = "hosts";

	public static final String IS_FUNDED_BY = "isFundedBy";
	public static final String FUNDS = "funds";

	public static final String FUNDINGS = "fundings";

	// community result uso isrelatedto

	public static final String RESULT_ENTITY = "result";
	public static final String DATASOURCE_ENTITY = "datasource";
	public static final String CONTEXT_ENTITY = "context";
	public static final String ORGANIZATION_ENTITY = "organization";
	public static final String PROJECT_ENTITY = "project";

	public static final String CONTEXT_ID = "00";
	public static final String CONTEXT_NS_PREFIX = "context____";

	public static final String HARVESTED = "Harvested";
	public static final String DEFAULT_TRUST = "0.9";
   // public static final String FUNDER_DS = "entityregistry::projects";
}
