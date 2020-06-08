
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;

public class BrokerConstants {

	public static final String OPEN_ACCESS = "OPEN";
	public static final String IS_MERGED_IN_CLASS = "isMergedIn";

	public static final List<Class<? extends Result>> RESULT_CLASSES = Arrays
		.asList(Publication.class, Dataset.class, Software.class, OtherResearchProduct.class);

}
