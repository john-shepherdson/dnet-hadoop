package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;
import java.util.List;

public class ScholixEntityId implements Serializable {
    private String name;
    private List<ScholixIdentifier> identifiers;

    public ScholixEntityId() {
    }

    public ScholixEntityId(String name, List<ScholixIdentifier> identifiers) {
        this.name = name;
        this.identifiers = identifiers;
    }

    public String getName() {
        return name;
    }

    public ScholixEntityId setName(String name) {
        this.name = name;
        return this;
    }

    public List<ScholixIdentifier> getIdentifiers() {
        return identifiers;
    }

    public ScholixEntityId setIdentifiers(List<ScholixIdentifier> identifiers) {
        this.identifiers = identifiers;
        return this;
    }
}
