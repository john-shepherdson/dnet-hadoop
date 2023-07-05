package eu.dnetlib.dhp.actionmanager.bipaffiliations.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AffiliationRelationModel implements Serializable {
    private String doi;
    private String rorId;
    private double confidence;
}
