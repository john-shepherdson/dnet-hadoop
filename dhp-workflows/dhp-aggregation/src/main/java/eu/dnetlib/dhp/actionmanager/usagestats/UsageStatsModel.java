
package eu.dnetlib.dhp.actionmanager.usagestats;

import java.io.Serializable;

public class UsageStatsModel implements Serializable {
	private String result_id;
	private Long downloads;
	private Long views;

	public String getResult_id() {
		return result_id;
	}

	public void setResult_id(String result_id) {
		this.result_id = result_id;
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
