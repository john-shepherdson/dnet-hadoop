
package eu.dnetlib.dhp.actionmanager.bipmodel;

import java.io.Serializable;
import java.util.List;

/**
 * Rewriting of the bipFinder input data by extracting the identifier of the result (doi)
 */

public class BipScore implements Serializable {
	private String id; // doi
	private List<Score> scoreList; // unit as given in the inputfile

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Score> getScoreList() {
		return scoreList;
	}

	public void setScoreList(List<Score> scoreList) {
		this.scoreList = scoreList;
	}
}
