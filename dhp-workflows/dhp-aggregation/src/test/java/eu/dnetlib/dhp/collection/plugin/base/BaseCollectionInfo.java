
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.Serializable;

public class BaseCollectionInfo implements Serializable {

	private static final long serialVersionUID = 5766333937429419647L;

	private String id;
	private String opendoarId;
	private String rorId;

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getOpendoarId() {
		return this.opendoarId;
	}

	public void setOpendoarId(final String opendoarId) {
		this.opendoarId = opendoarId;
	}

	public String getRorId() {
		return this.rorId;
	}

	public void setRorId(final String rorId) {
		this.rorId = rorId;
	}

}
