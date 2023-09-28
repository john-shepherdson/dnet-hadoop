
package eu.dnetlib.dhp.swh.models;

import java.util.Date;

import com.cloudera.com.fasterxml.jackson.annotation.JsonFormat;
import com.cloudera.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LastVisitData {

	private String type;
	private String date;

	@JsonProperty("snapshot")
	private String snapshotId;

	private String status;

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

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
