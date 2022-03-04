
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class PidScheme implements Serializable {
	private List<String> scheme;
	private Boolean has_pid_scheme;

	public List<String> getScheme() {
		return scheme;
	}

	public void setScheme(List<String> scheme) {
		this.scheme = scheme;
	}

	public Boolean getHas_pid_scheme() {
		return has_pid_scheme;
	}

	public void setHas_pid_scheme(Boolean has_pid_scheme) {
		this.has_pid_scheme = has_pid_scheme;
	}
}
