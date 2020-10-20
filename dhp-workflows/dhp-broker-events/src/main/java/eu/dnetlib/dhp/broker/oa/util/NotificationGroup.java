
package eu.dnetlib.dhp.broker.oa.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Notification;

public class NotificationGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 720996471281158977L;

	private List<Notification> data = new ArrayList<>();

	public NotificationGroup() {
	}

	public NotificationGroup(final List<Notification> data) {
		this.data = data;
	}

	public List<Notification> getData() {
		return data;
	}

	public void setData(final List<Notification> data) {
		this.data = data;
	}

	public NotificationGroup addElement(final Notification elem) {
		data.add(elem);
		return this;
	}

	public NotificationGroup addGroup(final NotificationGroup group) {
		data.addAll(group.getData());
		return this;
	}

}
