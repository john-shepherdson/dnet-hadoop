
package eu.dnetlib.dhp.swh.models;

import com.cloudera.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LastVisitResponse {

	private String type;

	private String date;

	@JsonProperty("snapshot")
	private String snapshotId;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSnapshot() {
		return snapshotId;
	}

	public void setSnapshot(String snapshotId) {
		this.snapshotId = snapshotId;
	}
}
