
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

	public static KeyValue newInstance(String key, String value) {
		KeyValue inst = new KeyValue();
		inst.key = key;
		inst.value = value;
		return inst;
	}

	@JsonIgnore
	public boolean isBlank() {
		return StringUtils.isBlank(key) && StringUtils.isBlank(value);
	}

}
