
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;

public class Work extends OrcidData implements Serializable {
	WorkDetail workDetail;

	public WorkDetail getWorkDetail() {
		return workDetail;
	}

	public void setWorkDetail(WorkDetail workDetail) {
		this.workDetail = workDetail;
	}
}
