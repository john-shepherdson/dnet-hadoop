
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import java.io.Serializable;

/**
 * @author dimitris.pierrakos
 * @Date 30/10/23
 */
public class StatsOAColourModel implements Serializable {
	private String id;
	private boolean is_gold;
	private boolean is_bronze_oa;
	private boolean is_hybrid;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isIs_gold() {
		return is_gold;
	}

	public void setIs_gold(boolean is_gold) {
		this.is_gold = is_gold;
	}

	public boolean isIs_bronze_oa() {
		return is_bronze_oa;
	}

	public void setIs_bronze_oa(boolean is_bronze_oa) {
		this.is_bronze_oa = is_bronze_oa;
	}

	public boolean isIs_hybrid() {
		return is_hybrid;
	}

	public void setIs_hybrid(boolean is_hybrid) {
		this.is_hybrid = is_hybrid;
	}
}
