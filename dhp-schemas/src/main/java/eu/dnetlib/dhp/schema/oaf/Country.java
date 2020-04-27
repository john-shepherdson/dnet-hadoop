
package eu.dnetlib.dhp.schema.oaf;

import java.util.Objects;

public class Country extends Qualifier {

	private DataInfo dataInfo;

	public DataInfo getDataInfo() {
		return dataInfo;
	}

	public void setDataInfo(DataInfo dataInfo) {
		this.dataInfo = dataInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;
		Country country = (Country) o;
		return Objects.equals(dataInfo, country.dataInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), dataInfo);
	}
}
