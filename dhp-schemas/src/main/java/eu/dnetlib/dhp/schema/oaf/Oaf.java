
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Oaf implements Serializable {

	/**
	 * The list of datasource id/name pairs providing this relationship.
	 */
	protected List<KeyValue> collectedfrom;

	private DataInfo dataInfo;

	private Long lastupdatetimestamp;

	public List<KeyValue> getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(List<KeyValue> collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public DataInfo getDataInfo() {
		return dataInfo;
	}

	public void setDataInfo(DataInfo dataInfo) {
		this.dataInfo = dataInfo;
	}

	public Long getLastupdatetimestamp() {
		return lastupdatetimestamp;
	}

	public void setLastupdatetimestamp(Long lastupdatetimestamp) {
		this.lastupdatetimestamp = lastupdatetimestamp;
	}

	public void mergeFrom(Oaf o) {
		if (Objects.isNull(o)) {
			return;
		}
		setCollectedfrom(
			Stream
				.concat(
					Optional
						.ofNullable(getCollectedfrom())
						.map(Collection::stream)
						.orElse(Stream.empty()),
					Optional
						.ofNullable(o.getCollectedfrom())
						.map(Collection::stream)
						.orElse(Stream.empty()))
				.distinct() // relies on KeyValue.equals
				.collect(Collectors.toList()));

		mergeOAFDataInfo(o);

		setLastupdatetimestamp(
			Math
				.max(
					Optional.ofNullable(getLastupdatetimestamp()).orElse(0L),
					Optional.ofNullable(o.getLastupdatetimestamp()).orElse(0L)));
	}

	public void mergeOAFDataInfo(Oaf o) {
		if (o.getDataInfo() != null && compareTrust(this, o) < 0)
			dataInfo = o.getDataInfo();
	}

	protected String extractTrust(Oaf e) {
		if (e == null || e.getDataInfo() == null || e.getDataInfo().getTrust() == null)
			return "0.0";
		return e.getDataInfo().getTrust();
	}

	protected int compareTrust(Oaf a, Oaf b) {
		return extractTrust(a).compareTo(extractTrust(b));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Oaf oaf = (Oaf) o;
		return Objects.equals(getDataInfo(), oaf.getDataInfo())
			&& Objects.equals(lastupdatetimestamp, oaf.lastupdatetimestamp);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataInfo, lastupdatetimestamp);
	}
}
