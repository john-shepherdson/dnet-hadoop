
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;

import eu.dnetlib.broker.api.ShortEventMessage;

public class ShortEventMessageWithGroupId extends ShortEventMessage implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 4704889388757626630L;

	private String group;

	public String getGroup() {
		return group;
	}

	public void setGroup(final String group) {
		this.group = group;
	}

}
