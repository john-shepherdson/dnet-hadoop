
package eu.dnetlib.dhp.broker.oa.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.broker.model.OaNotification;

public class OaNotificationGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 720996471281158977L;

	private List<OaNotification> data = new ArrayList<>();

	public OaNotificationGroup() {}

	public OaNotificationGroup(final List<OaNotification> data) {
		this.data = data;
	}

	public List<OaNotification> getData() {
		return this.data;
	}

	public void setData(final List<OaNotification> data) {
		this.data = data;
	}

	public OaNotificationGroup addElement(final OaNotification elem) {
		this.data.add(elem);
		return this;
	}

	public OaNotificationGroup addGroup(final OaNotificationGroup group) {
		this.data.addAll(group.getData());
		return this;
	}

}
