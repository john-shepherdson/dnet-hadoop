package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;
import java.util.List;

public class ScholixEntityId implements Serializable {
    private String name;
    private List<ScholixIdentifier> identifiers;

    public ScholixEntityId() {}

    public ScholixEntityId(String name, List<ScholixIdentifier> identifiers) {
        this.name = name;
        this.identifiers = identifiers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ScholixIdentifier> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<ScholixIdentifier> identifiers) {
        this.identifiers = identifiers;
    }
}
