
package eu.dnetlib.dhp.actionmanager.bipaffiliations.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class AffiliationRelationDeserializer implements Serializable {
	@JsonProperty("DOI")
	private String doi;
	@JsonProperty("Matchings")
	private List<Matching> matchings;

	@Data
	public static class Matching implements Serializable {
		@JsonProperty("RORid")
		private List<String> rorId;
		@JsonProperty("Confidence")
		private double confidence;

	}

}
