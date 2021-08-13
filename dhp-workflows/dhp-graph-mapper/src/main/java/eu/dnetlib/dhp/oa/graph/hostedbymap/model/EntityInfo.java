
package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;

public class EntityInfo implements Serializable {
	private String id;
	private String journalId;
	private String name;
	private Boolean openAccess;
	private String hostedById;

	public static EntityInfo newInstance(String id, String journalId, String name) {
		return newInstance(id, journalId, name, false);

	}

	public static EntityInfo newInstance(String id, String journalId, String name, Boolean openaccess) {
		EntityInfo pi = new EntityInfo();
		pi.id = id;
		pi.journalId = journalId;
		pi.name = name;
		pi.openAccess = openaccess;
		pi.hostedById = "";
		return pi;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getJournalId() {
		return journalId;
	}

	public void setJournalId(String journalId) {
		this.journalId = journalId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean getOpenAccess() {
		return openAccess;
	}

	public void setOpenAccess(Boolean openAccess) {
		this.openAccess = openAccess;
	}

	public String getHostedById() {
		return hostedById;
	}

	public void setHostedById(String hostedById) {
		this.hostedById = hostedById;
	}
}
