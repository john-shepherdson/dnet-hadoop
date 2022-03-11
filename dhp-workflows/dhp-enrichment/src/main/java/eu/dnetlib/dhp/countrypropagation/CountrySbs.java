
package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;

public class CountrySbs implements Serializable {
	private String classid;
	private String classname;

	public String getClassid() {
		return classid;
	}

	public void setClassid(String classid) {
		this.classid = classid;
	}

	public String getClassname() {
		return classname;
	}

	public void setClassname(String classname) {
		this.classname = classname;
	}

	public static CountrySbs newInstance(String classid, String classname) {
		CountrySbs csbs = new CountrySbs();
		csbs.classid = classid;
		csbs.classname = classname;
		return csbs;
	}
}
