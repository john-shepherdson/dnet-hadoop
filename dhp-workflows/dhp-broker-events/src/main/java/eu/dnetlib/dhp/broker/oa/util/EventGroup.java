
package eu.dnetlib.dhp.broker.oa.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Event;

public class EventGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 765977943803533130L;

	private final List<Event> data = new ArrayList<>();

	public List<Event> getData() {
		return data;
	}

	public EventGroup addElement(final Event elem) {
		data.add(elem);
		return this;
	}

	public EventGroup addGroup(final EventGroup group) {
		data.addAll(group.getData());
		return this;
	}

}
