package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class OAIProvenance implements Serializable {

    private OriginDescription originDescription;

    public OriginDescription getOriginDescription() {
        return originDescription;
    }

    public void setOriginDescription(OriginDescription originDescription) {
        this.originDescription = originDescription;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OAIProvenance that = (OAIProvenance) o;
        return Objects.equals(originDescription, that.originDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originDescription);
    }
}
