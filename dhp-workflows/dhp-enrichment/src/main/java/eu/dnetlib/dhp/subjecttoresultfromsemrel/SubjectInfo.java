
package eu.dnetlib.dhp.subjecttoresultfromsemrel;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 06/10/22
 */
public class SubjectInfo implements Serializable {
	private String classid;
	private String value;
	private String classname;

	public static SubjectInfo newInstance(String classid, String classname, String value) {
		SubjectInfo si = new SubjectInfo();
		si.classid = classid;
		si.value = value;
		si.classname = classname;
		return si;
	}

	public String getClassid() {
		return classid;
	}

	public void setClassid(String classid) {
		this.classid = classid;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getClassname() {
		return classname;
	}

	public void setClassname(String classname) {
		this.classname = classname;
	}
}
