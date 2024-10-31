
package eu.dnetlib.dhp.schema.oaf;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class HashableKeyValue extends KeyValue {

	public static HashableKeyValue newInstance(String key, String value) {
		if (value == null) {
			return null;
		}
		final HashableKeyValue kv = new HashableKeyValue();
		kv.setValue(value);
		kv.setKey(key);

		return kv;
	}

	public static HashableKeyValue newInstance(KeyValue kv) {
		HashableKeyValue hkv = new HashableKeyValue();
		hkv.setKey(kv.getKey());
		hkv.setValue(kv.getValue());

		return hkv;
	}

	public static KeyValue toKeyValue(HashableKeyValue hkv) {
		KeyValue kv = new KeyValue();
		kv.setKey(hkv.getKey());
		kv.setValue(hkv.getValue());

		return kv;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(11, 91)
			.append(getKey())
			.append(getValue())
			.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != getClass()) {
			return false;
		}
		final HashableKeyValue rhs = (HashableKeyValue) obj;
		return new EqualsBuilder()
			.append(getKey(), rhs.getKey())
			.append(getValue(), rhs.getValue())
			.isEquals();
	}
}
