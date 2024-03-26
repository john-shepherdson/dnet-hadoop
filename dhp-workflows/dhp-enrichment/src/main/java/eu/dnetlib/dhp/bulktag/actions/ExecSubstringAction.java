
package eu.dnetlib.dhp.bulktag.actions;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 19/01/24
 */
public class ExecSubstringAction implements Serializable {

	private String value;
	private String from;
	private String to;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String execSubstring() {
		return this.value.substring(Integer.valueOf(this.from), Integer.valueOf(this.to));

	}
}
