
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class OriginDescription implements Serializable {

	private String harvestDate;

	private Boolean altered = true;

	private String baseURL;

	private String identifier;

	private String datestamp;

	private String metadataNamespace;

	public String getHarvestDate() {
		return harvestDate;
	}

	public void setHarvestDate(String harvestDate) {
		this.harvestDate = harvestDate;
	}

	public Boolean getAltered() {
		return altered;
	}

	public void setAltered(Boolean altered) {
		this.altered = altered;
	}

	public String getBaseURL() {
		return baseURL;
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getDatestamp() {
		return datestamp;
	}

	public void setDatestamp(String datestamp) {
		this.datestamp = datestamp;
	}

	public String getMetadataNamespace() {
		return metadataNamespace;
	}

	public void setMetadataNamespace(String metadataNamespace) {
		this.metadataNamespace = metadataNamespace;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		OriginDescription that = (OriginDescription) o;
		return Objects.equals(harvestDate, that.harvestDate)
			&& Objects.equals(altered, that.altered)
			&& Objects.equals(baseURL, that.baseURL)
			&& Objects.equals(identifier, that.identifier)
			&& Objects.equals(datestamp, that.datestamp)
			&& Objects.equals(metadataNamespace, that.metadataNamespace);
	}

	@Override
	public int hashCode() {
		return Objects.hash(harvestDate, altered, baseURL, identifier, datestamp, metadataNamespace);
	}
}
