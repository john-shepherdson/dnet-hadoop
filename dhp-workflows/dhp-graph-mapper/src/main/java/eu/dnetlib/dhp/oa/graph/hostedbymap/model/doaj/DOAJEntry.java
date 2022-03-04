
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class DOAJEntry implements Serializable {
	private String last_updated;
	private BibJson bibjson;
	private Admin admin;
	private String created_date;
	private String id;

	public String getLast_updated() {
		return last_updated;
	}

	public void setLast_updated(String last_updated) {
		this.last_updated = last_updated;
	}

	public BibJson getBibjson() {
		return bibjson;
	}

	public void setBibjson(BibJson bibjson) {
		this.bibjson = bibjson;
	}

	public Admin getAdmin() {
		return admin;
	}

	public void setAdmin(Admin admin) {
		this.admin = admin;
	}

	public String getCreated_date() {
		return created_date;
	}

	public void setCreated_date(String created_date) {
		this.created_date = created_date;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
