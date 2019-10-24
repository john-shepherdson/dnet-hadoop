package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class OAIProvenance  implements Serializable {

    private OriginDescription originDescription;

    public OriginDescription getOriginDescription() {
        return originDescription;
    }

    public void setOriginDescription(OriginDescription originDescription) {
        this.originDescription = originDescription;
    }
}
