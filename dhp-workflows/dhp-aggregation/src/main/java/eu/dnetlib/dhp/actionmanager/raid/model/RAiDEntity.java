
package eu.dnetlib.dhp.actionmanager.raid.model;

import java.io.Serializable;
import java.util.List;

public class RAiDEntity implements Serializable {

	String id;
	String title;
	String description;
	List<String> ids;
	String startDate;
	String endDate;

	public RAiDEntity() {
	}

	public RAiDEntity(String id, String title, String description, List<String> ids, String startDate, String endDate) {
		this.id = id;
		this.title = title;
		this.description = description;
		this.ids = ids;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setIds(List<String> ids) {
		this.ids = ids;
	}

	public List<String> getIds() {
		return ids;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
}
