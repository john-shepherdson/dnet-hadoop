
package eu.dnetlib.dhp.bulktag.actions;

import java.io.Serializable;
import java.util.List;

/**
 * @author miriam.baglioni
 * @Date 22/01/24
 */
public class Action implements Serializable {
	private String clazz;
	private String method;
	private List<Parameters> params;

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public List<Parameters> getParams() {
		return params;
	}

	public void setParams(List<Parameters> params) {
		this.params = params;
	}
}
