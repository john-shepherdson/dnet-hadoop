
package eu.dnetlib.dhp.oa.provision;

public class ProvisionConstants {

	public static final String LAYOUT = "index";
	public static final String INTERPRETATION = "openaire";
	public static final String SEPARATOR = "-";

	public static String getCollectionName(String format) {
		return format + SEPARATOR + LAYOUT + SEPARATOR + INTERPRETATION;
	}

}
