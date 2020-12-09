
package eu.dnetlib.dhp.schema.orcid;

public class Work extends OrcidData {
	WorkDetail workDetail;

	public WorkDetail getWorkDetail() {
		return workDetail;
	}

	public void setWorkDetail(WorkDetail workDetail) {
		this.workDetail = workDetail;
	}
}
