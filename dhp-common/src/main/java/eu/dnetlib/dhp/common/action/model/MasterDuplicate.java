
package eu.dnetlib.dhp.common.action.model;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 21/07/22
 */
public class MasterDuplicate implements Serializable {
	private String duplicate;
	private String master;

	public String getDuplicate() {
		return duplicate;
	}

	public void setDuplicate(String duplicate) {
		this.duplicate = duplicate;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}
}
