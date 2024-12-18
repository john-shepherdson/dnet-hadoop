
package eu.dnetlib.dhp.bulktag.actions;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 22/01/24
 */
public class MapModel implements Serializable {

	private String path;
	private Action action;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Action getAction() {
		return action;
	}

	public void setAction(Action action) {
		this.action = action;
	}
}
