
package eu.dnetlib.dhp.broker.oa.util.aggregators.simple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;

public class ResultGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -3360828477088669296L;

	private final List<OpenaireBrokerResult> data = new ArrayList<>();

	public List<OpenaireBrokerResult> getData() {
		return data;
	}

	public ResultGroup addElement(final OpenaireBrokerResult elem) {
		data.add(elem);
		return this;
	}

	public ResultGroup addGroup(final ResultGroup group) {
		data.addAll(group.getData());
		return this;
	}

	public boolean isValid() {
		return data.size() > 1;
	}
}
