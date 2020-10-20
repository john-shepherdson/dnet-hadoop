
package eu.dnetlib.dhp.schema.dump.oaf;

/**
 * AccessRight. Used to represent the result access rights. It extends the eu.dnet.lib.dhp.schema.dump.oaf.Qualifier
 * element with a parameter scheme of type String to store the scheme. Values for this element are found against the
 * COAR access right scheme. The classid of the element accessright in eu.dnetlib.dhp.schema.oaf.Result is used to get
 * the COAR corresponding code whose value will be used to set the code parameter. The COAR label corresponding to the
 * COAR code will be used to set the label parameter. The scheme value will always be the one referring to the COAR
 * access right scheme
 */
public class AccessRight extends Qualifier {

	private String scheme;

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public static AccessRight newInstance(String code, String label, String scheme) {
		AccessRight ar = new AccessRight();
		ar.setCode(code);
		ar.setLabel(label);
		ar.setScheme(scheme);
		return ar;
	}
}
