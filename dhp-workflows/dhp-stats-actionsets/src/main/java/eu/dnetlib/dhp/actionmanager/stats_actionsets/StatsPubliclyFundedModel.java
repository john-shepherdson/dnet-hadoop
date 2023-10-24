
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import java.io.Serializable;

/**
 * @author dimitris.pierrakos
 * @Date 30/10/23
 */
public class StatsPubliclyFundedModel implements Serializable {
	private String id;
	private boolean publicly_funded;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isPublicly_funded() {
		return publicly_funded;
	}

	public void setPublicly_funded(boolean publicly_funded) {
		this.publicly_funded = publicly_funded;
	}
}
