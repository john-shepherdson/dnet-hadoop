
package eu.dnetlib.dhp.swh.models;

import com.cloudera.com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LastVisitData {

	private String type;

	private Date date;

	@JsonProperty("snapshot")
	private String snapshotId;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getSnapshot() {
		return snapshotId;
	}

	public void setSnapshot(String snapshotId) {
		this.snapshotId = snapshotId;
	}
}
