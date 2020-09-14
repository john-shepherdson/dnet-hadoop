
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * To represent the information described by a key and a value. It has two parameters: - key to store the key (generally
 * the OpenAIRE id for some entity) - value to store the value (generally the OpenAIRE name for the key)
 */
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
