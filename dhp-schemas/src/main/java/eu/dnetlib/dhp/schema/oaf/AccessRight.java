
package eu.dnetlib.dhp.schema.oaf;

import java.util.Optional;

/**
 * This class models the access rights of research products.
 */
public class AccessRight extends Qualifier {

	private OAStatus oaStatus;

	public OAStatus getOaStatus() {
		return oaStatus;
	}

	public void setOaStatus(OAStatus oaStatus) {
		this.oaStatus = oaStatus;
	}

	public String toComparableString() {
		String s = super.toComparableString();
		return Optional
			.ofNullable(getOaStatus())
			.map(x -> s + "::" + x.toString())
			.orElse(s);
	}

	@Override
	public int hashCode() {
		return toComparableString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		Qualifier other = (Qualifier) obj;

		return toComparableString().equals(other.toComparableString());
	}

}
