
package eu.dnetlib.dhp.swh.models;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LastVisitData implements Serializable {

	private String origin;
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

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	@Override
	public String toString() {
		return "LastVisitData{" +
			"origin='" + origin + '\'' +
			", type='" + type + '\'' +
			", date='" + date + '\'' +
			", snapshotId='" + snapshotId + '\'' +
			", status='" + status + '\'' +
			'}';
	}
}
