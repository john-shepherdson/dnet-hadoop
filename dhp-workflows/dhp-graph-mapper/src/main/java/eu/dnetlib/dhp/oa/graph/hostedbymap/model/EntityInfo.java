
package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;

public class EntityInfo implements Serializable {
	private String id;
	private String journal_id;
	private String name;
	private Boolean openaccess;
	private String hb_id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getJournal_id() {
		return journal_id;
	}

	public void setJournal_id(String journal_id) {
		this.journal_id = journal_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean getOpenaccess() {
		return openaccess;
	}

	public void setOpenaccess(Boolean openaccess) {
		this.openaccess = openaccess;
	}

	public String getHb_id() {
		return hb_id;
	}

	public void setHb_id(String hb_id) {
		this.hb_id = hb_id;
	}

	public static EntityInfo newInstance(String id, String j_id, String name) {
		return newInstance(id, j_id, name, false);

	}

	public static EntityInfo newInstance(String id, String j_id, String name, Boolean openaccess) {
		EntityInfo pi = new EntityInfo();
		pi.id = id;
		pi.journal_id = j_id;
		pi.name = name;
		pi.openaccess = openaccess;
		pi.hb_id = "";
		return pi;

	}
}
