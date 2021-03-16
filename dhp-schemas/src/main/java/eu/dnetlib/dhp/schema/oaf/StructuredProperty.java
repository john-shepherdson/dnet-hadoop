
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;

public class StructuredProperty implements Serializable {

	private String value;

	private Qualifier qualifier;

	private DataInfo dataInfo;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Qualifier getQualifier() {
		return qualifier;
	}

	public void setQualifier(Qualifier qualifier) {
		this.qualifier = qualifier;
	}

	public DataInfo getDataInfo() {
		return dataInfo;
	}

	public void setDataInfo(DataInfo dataInfo) {
		this.dataInfo = dataInfo;
	}

	public String toComparableString() {
		return Stream
			.of(
				getQualifier().toComparableString(),
				Optional.ofNullable(getValue()).map(String::toLowerCase).orElse(""))
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.joining("||"));
	}

	@Override
	public int hashCode() {
		return toComparableString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		StructuredProperty other = (StructuredProperty) obj;

		return toComparableString().equals(other.toComparableString());
	}
}
