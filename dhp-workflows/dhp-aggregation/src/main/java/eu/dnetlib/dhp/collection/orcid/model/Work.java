
package eu.dnetlib.dhp.collection.orcid.model;

import java.util.ArrayList;
import java.util.List;

public class Work extends ORCIDItem {

	private String title;

	private List<Pid> pids;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Pid> getPids() {
		return pids;
	}

	public void setPids(List<Pid> pids) {
		this.pids = pids;
	}

	public void addPid(Pid pid) {
		if (pids == null)
			pids = new ArrayList<>();
		pids.add(pid);
	}

	public Work() {
	}
}
