
package eu.dnetlib.dhp.actionmanager.bipaffiliations.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class AffiliationRelationModel implements Serializable {
	private String doi;
	private String rorId;
	private double confidence;
}
