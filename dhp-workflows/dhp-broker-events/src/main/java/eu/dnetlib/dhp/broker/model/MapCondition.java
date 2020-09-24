
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MapCondition implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -7137490975452466813L;

	private String field;
	private List<ConditionParams> listParams = new ArrayList<>();

	public String getField() {
		return field;
	}

	public void setField(final String field) {
		this.field = field;
	}

	public List<ConditionParams> getListParams() {
		return listParams;
	}

	public void setListParams(final List<ConditionParams> listParams) {
		this.listParams = listParams;
	}

}
