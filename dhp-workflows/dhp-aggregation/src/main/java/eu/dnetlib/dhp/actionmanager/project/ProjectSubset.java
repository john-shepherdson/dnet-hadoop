
package eu.dnetlib.dhp.actionmanager.project;

import java.io.Serializable;

/**
 * Class to store the grande agreement (code) of the collected projects
 */
public class ProjectSubset implements Serializable {

	private String code;


	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

}
