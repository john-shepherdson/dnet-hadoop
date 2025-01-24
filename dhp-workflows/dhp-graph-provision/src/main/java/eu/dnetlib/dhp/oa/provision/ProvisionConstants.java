
package eu.dnetlib.dhp.oa.provision;

public class ProvisionConstants {

	private ProvisionConstants() {
	}

	public static final String LAYOUT = "index";
	public static final String INTERPRETATION = "openaire";
	public static final String SEPARATOR = "-";

	public static String getCollectionName(String format) {
		return format + SEPARATOR + LAYOUT + SEPARATOR + INTERPRETATION;
	}

	public static final String PUBLIC_ALIAS_NAME = "public";
	public static final String SHADOW_ALIAS_NAME = "shadow";

}
