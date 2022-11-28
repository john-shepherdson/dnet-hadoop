
package eu.dnetlib.dhp.oa.graph.clean.cfhb;

import java.io.Serializable;

public class IdCfHbMapping implements Serializable {

	private String resultid;

	private String cfhb;

	private String master;

	public IdCfHbMapping() {
	}

	public IdCfHbMapping(String id) {
		this.resultid = id;
	}

	public String getResultid() {
		return resultid;
	}

	public void setResultid(String resultid) {
		this.resultid = resultid;
	}

	public String getCfhb() {
		return cfhb;
	}

	public void setCfhb(String cfhb) {
		this.cfhb = cfhb;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}
}
