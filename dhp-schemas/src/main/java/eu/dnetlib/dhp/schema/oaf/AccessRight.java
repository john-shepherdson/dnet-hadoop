
package eu.dnetlib.dhp.schema.oaf;

import java.util.Optional;

/**
 * This class models the access rights of research products.
 */
public class AccessRight extends Qualifier {

	private OpenAccessRoute openAccessRoute;

	public OpenAccessRoute getOpenAccessRoute() {
		return openAccessRoute;
	}

	public void setOpenAccessRoute(OpenAccessRoute openAccessRoute) {
		this.openAccessRoute = openAccessRoute;
	}

	public String toComparableString() {
		String s = super.toComparableString();
		return Optional
			.ofNullable(getOpenAccessRoute())
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
