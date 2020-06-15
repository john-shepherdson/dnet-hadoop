
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class Oaf implements Serializable {

	/**
	 * The list of datasource id/name pairs providing this relationship.
	 */
	protected List<KeyValue> collectedfrom;

	private Long lastupdatetimestamp;

	public List<KeyValue> getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(List<KeyValue> collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public Long getLastupdatetimestamp() {
		return lastupdatetimestamp;
	}

	public void setLastupdatetimestamp(Long lastupdatetimestamp) {
		this.lastupdatetimestamp = lastupdatetimestamp;
	}

//	public void setAllowedValues(eu.dnetlib.dhp.schema.oaf.Oaf o){
//		collectedfrom = o.getCollectedfrom().stream().map(cf -> KeyValue.newInstance(cf)).collect(Collectors.toList());
//
//		lastupdatetimestamp = o.getLastupdatetimestamp();
//
//	}

}
