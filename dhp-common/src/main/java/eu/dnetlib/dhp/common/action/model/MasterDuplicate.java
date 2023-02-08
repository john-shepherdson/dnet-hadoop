
package eu.dnetlib.dhp.common.action.model;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 21/07/22
 */
public class MasterDuplicate implements Serializable {
	private String duplicateId;
	private String masterId;
	private String masterName;

	public String getDuplicateId() {
		return duplicateId;
	}

	public void setDuplicateId(String duplicateId) {
		this.duplicateId = duplicateId;
	}

	public String getMasterId() {
		return masterId;
	}

	public void setMasterId(String masterId) {
		this.masterId = masterId;
	}

	public String getMasterName() {
		return masterName;
	}

	public void setMasterName(String masterName) {
		this.masterName = masterName;
	}
}
