
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = ExternalIdTypeDeserializer.class)
public class ExternalIdType implements Serializable {

	private List<String> all;

	private String preferred;

	private final static long serialVersionUID = 2616688352998387611L;

	public ExternalIdType() {
	}

	public ExternalIdType(final List<String> all, final String preferred) {
		this.all = all;
		this.preferred = preferred;
	}

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
