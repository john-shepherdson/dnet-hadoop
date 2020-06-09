package eu.dnetlib.dhp.broker.oa.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Result;

public class ResultGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -3360828477088669296L;

	private final List<Result> data = new ArrayList<>();

	public List<Result> getData() {
		return data;
	}

	public ResultGroup addElement(final Result elem) {
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
