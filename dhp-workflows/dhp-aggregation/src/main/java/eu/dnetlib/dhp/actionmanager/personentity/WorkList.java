
package eu.dnetlib.dhp.actionmanager.personentity;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import eu.dnetlib.dhp.collection.orcid.model.Work;

public class WorkList implements Serializable {
	private ArrayList<Work> workArrayList;

	public ArrayList<Work> getWorkArrayList() {
		return workArrayList;
	}

	public void setWorkArrayList(ArrayList<Work> workArrayList) {
		this.workArrayList = workArrayList;
	}

	public WorkList() {
		workArrayList = new ArrayList<>();
	}
}
