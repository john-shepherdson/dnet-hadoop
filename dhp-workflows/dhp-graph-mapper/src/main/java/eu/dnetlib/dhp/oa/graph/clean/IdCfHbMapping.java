
package eu.dnetlib.dhp.oa.graph.clean;

import java.io.Serializable;

public class IdCfHbMapping implements Serializable {

	private String resultId;

	private String cfhb;

	private String masterId;

	private String masterName;

	public IdCfHbMapping() {
	}

	public IdCfHbMapping(String id) {
		this.resultId = id;
	}

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public String getCfhb() {
		return cfhb;
	}

	public void setCfhb(String cfhb) {
		this.cfhb = cfhb;
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
