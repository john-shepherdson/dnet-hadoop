
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import java.io.Serializable;

/**
 * @author dimitris.pierrakos
 * @Date 30/10/23
 */
public class StatsGreenOAModel implements Serializable {
	private String id;
	private boolean green_oa;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isGreen_oa() {
		return green_oa;
	}

	public void setGreen_oa(boolean green_oa) {
		this.green_oa = green_oa;
	}
}
