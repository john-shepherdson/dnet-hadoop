
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class Admin implements Serializable {
	private Boolean ticked;
	private Boolean seal;

	public Boolean getTicked() {
		return ticked;
	}

	public void setTicked(Boolean ticked) {
		this.ticked = ticked;
	}

	public Boolean getSeal() {
		return seal;
	}

	public void setSeal(Boolean seal) {
		this.seal = seal;
	}
}
