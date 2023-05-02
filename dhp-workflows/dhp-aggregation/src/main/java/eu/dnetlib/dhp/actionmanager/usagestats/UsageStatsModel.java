
package eu.dnetlib.dhp.actionmanager.usagestats;

import java.io.Serializable;

public class UsageStatsModel implements Serializable {
	private String id;
	private Long downloads;
	private Long views;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getDownloads() {
		return downloads;
	}

	public void setDownloads(Long downloads) {
		this.downloads = downloads;
	}

	public Long getViews() {
		return views;
	}

	public void setViews(Long views) {
		this.views = views;
	}
}
