
package eu.dnetlib.dhp.broker.oa.util.aggregators.simple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class ResultGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -3360828477088669296L;

	private final List<ResultWithRelations> data = new ArrayList<>();

	public List<ResultWithRelations> getData() {
		return data;
	}

	public ResultGroup addElement(final ResultWithRelations elem) {
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
