
package eu.dnetlib.dhp.schema.dump.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class KeyValue implements Serializable {

	private String key;

	private String value;


	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}



	@JsonIgnore
	public boolean isBlank() {
		return StringUtils.isBlank(key) && StringUtils.isBlank(value);
	}


}
