
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.oaf.*;

/**
 * @author dimitris.pierrakos
 * @Date 30/10/23
 */
public class StatsResultEnhancementModel implements Serializable {
	private String id;
	private Boolean is_gold;
	private Boolean is_bronze_oa;
	private Boolean is_hybrid;
	private boolean in_diamond_journal;
	private boolean green_oa;
	private boolean publicly_funded;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Boolean isIs_gold() {
		return is_gold;
	}

	public void setIs_gold(Boolean is_gold) {
		this.is_gold = is_gold;
	}

	public Boolean isIs_bronze_oa() {
		return is_bronze_oa;
	}

	public void setIs_bronze_oa(Boolean is_bronze_oa) {
		this.is_bronze_oa = is_bronze_oa;
	}

	public Boolean isIs_hybrid() {
		return is_hybrid;
	}

	public void setIs_hybrid(Boolean is_hybrid) {
		this.is_hybrid = is_hybrid;
	}

	public boolean isIn_diamond_journal() {
		return in_diamond_journal;
	}

	public void setIn_diamond_journal(boolean in_diamond_journal) {
		this.in_diamond_journal = in_diamond_journal;
	}

	public boolean isGreen_oa() {
		return green_oa;
	}

	public void setGreen_oa(boolean green_oa) {
		this.green_oa = green_oa;
	}

	public boolean isPublicly_funded() {
		return publicly_funded;
	}

	public void setPublicly_funded(boolean publicly_funded) {
		this.publicly_funded = publicly_funded;
	}
}
