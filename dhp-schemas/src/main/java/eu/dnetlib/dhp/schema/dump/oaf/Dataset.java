
package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.common.ModelConstants;


import java.io.Serializable;
import java.util.List;

public class Dataset extends Result implements Serializable {


	private String size;

	private String version;

	private List<GeoLocation> geolocation;

	public Dataset() {
		setType(ModelConstants.DATASET_DEFAULT_RESULTTYPE.getClassname());
	}


	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public List<GeoLocation> getGeolocation() {
		return geolocation;
	}

	public void setGeolocation(List<GeoLocation> geolocation) {
		this.geolocation = geolocation;
	}


}
