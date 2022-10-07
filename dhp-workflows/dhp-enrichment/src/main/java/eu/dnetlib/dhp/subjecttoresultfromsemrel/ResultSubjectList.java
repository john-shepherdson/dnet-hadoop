
package eu.dnetlib.dhp.subjecttoresultfromsemrel;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.dnetlib.dhp.schema.oaf.Subject;

/**
 * @author miriam.baglioni
 * @Date 04/10/22
 */
public class ResultSubjectList implements Serializable {
	private String resId;
	List<SubjectInfo> subjectList;

	public String getResId() {
		return resId;
	}

	public void setResId(String resId) {
		this.resId = resId;
	}

	public List<SubjectInfo> getSubjectList() {
		return subjectList;
	}

	public void setSubjectList(List<SubjectInfo> subjectList) {
		this.subjectList = subjectList;
	}
}
