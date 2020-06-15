
package eu.dnetlib.dhp.schema.dump.oaf;

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
