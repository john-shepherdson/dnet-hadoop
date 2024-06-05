
package eu.dnetlib.dhp.oa.oaipmh;

import java.io.Serializable;
import java.util.List;

public class OaiRecordWrapper implements Serializable {

	private static final long serialVersionUID = 8997046455575004880L;

	private String id;
	private byte[] body;
	private String date;
	private List<String> sets;

	public OaiRecordWrapper() {
	}

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public byte[] getBody() {
		return this.body;
	}

	public void setBody(final byte[] body) {
		this.body = body;
	}

	public String getDate() {
		return this.date;
	}

	public void setDate(final String date) {
		this.date = date;
	}

	public List<String> getSets() {
		return this.sets;
	}

	public void setSets(final List<String> sets) {
		this.sets = sets;
	}

}
