
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

public class SimpleDatasourceInfo implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 2996609859416024734L;

	private String id;
	private String type;

	public SimpleDatasourceInfo() {
	}

	public SimpleDatasourceInfo(final String id, final String type) {
		this.id = id;
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

}
