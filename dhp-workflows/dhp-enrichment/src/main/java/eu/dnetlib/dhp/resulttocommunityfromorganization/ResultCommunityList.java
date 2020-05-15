
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import java.io.Serializable;
import java.util.ArrayList;

public class ResultCommunityList implements Serializable {
	private String resultId;
	private ArrayList<String> communityList;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public ArrayList<String> getCommunityList() {
		return communityList;
	}

	public void setCommunityList(ArrayList<String> communityList) {
		this.communityList = communityList;
	}
}
