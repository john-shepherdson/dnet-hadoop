
package eu.dnetlib.dhp.collection.orcid.model;

public class Employment extends ORCIDItem {

	private String startDate;
	private String EndDate;

	private Pid affiliationId;

	private String departmentName;

	private String roleTitle;

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return EndDate;
	}

	public void setEndDate(String endDate) {
		EndDate = endDate;
	}

	public Pid getAffiliationId() {
		return affiliationId;
	}

	public void setAffiliationId(Pid affiliationId) {
		this.affiliationId = affiliationId;
	}

	public String getDepartmentName() {
		return departmentName;
	}

	public void setDepartmentName(String departmentName) {
		this.departmentName = departmentName;
	}

	public String getRoleTitle() {
		return roleTitle;
	}

	public void setRoleTitle(String roleTitle) {
		this.roleTitle = roleTitle;
	}
}
