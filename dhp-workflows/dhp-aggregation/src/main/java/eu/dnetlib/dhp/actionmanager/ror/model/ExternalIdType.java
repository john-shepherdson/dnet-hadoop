
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExternalIdType implements Serializable {

	private List<String> all = new ArrayList<>();
	private String preferred;
	private final static long serialVersionUID = 2616688352998387611L;

	public List<String> getAll() {
		return all;
	}

	public void setAll(final List<String> all) {
		this.all = all;
	}

	public String getPreferred() {
		return preferred;
	}

	public void setPreferred(final String preferred) {
		this.preferred = preferred;
	}

}
