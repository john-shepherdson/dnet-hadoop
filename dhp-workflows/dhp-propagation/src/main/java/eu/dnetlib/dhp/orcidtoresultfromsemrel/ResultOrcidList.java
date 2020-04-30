
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ResultOrcidList implements Serializable {
	String resultId;
	List<AutoritativeAuthor> authorList = new ArrayList<>();

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<AutoritativeAuthor> getAuthorList() {
		return authorList;
	}

	public void setAuthorList(List<AutoritativeAuthor> authorList) {
		this.authorList = authorList;
	}
}
