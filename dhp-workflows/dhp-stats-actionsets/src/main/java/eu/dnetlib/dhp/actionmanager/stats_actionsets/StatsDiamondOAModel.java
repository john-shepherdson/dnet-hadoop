
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import java.io.Serializable;

/**
 * @author dimitris.pierrakos
 * @Date 30/10/23
 */
public class StatsDiamondOAModel implements Serializable {
	private String id;
	private boolean in_diamond_journal;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isIn_diamond_journal() {
		return in_diamond_journal;
	}

	public void setIn_diamond_journal(boolean in_diamond_journal) {
		this.in_diamond_journal = in_diamond_journal;
	}
}
