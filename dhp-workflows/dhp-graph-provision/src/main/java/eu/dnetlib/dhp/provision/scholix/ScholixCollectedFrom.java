package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;

public class ScholixCollectedFrom implements Serializable {

    private ScholixEntityId provider;
    private String provisionMode;
    private String completionStatus;

    public ScholixCollectedFrom() {
    }

    public ScholixCollectedFrom(ScholixEntityId provider, String provisionMode, String completionStatus) {
        this.provider = provider;
        this.provisionMode = provisionMode;
        this.completionStatus = completionStatus;
    }

    public ScholixEntityId getProvider() {
        return provider;
    }

    public ScholixCollectedFrom setProvider(ScholixEntityId provider) {
        this.provider = provider;
        return this;
    }

    public String getProvisionMode() {
        return provisionMode;
    }

    public ScholixCollectedFrom setProvisionMode(String provisionMode) {
        this.provisionMode = provisionMode;
        return this;
    }

    public String getCompletionStatus() {
        return completionStatus;
    }

    public ScholixCollectedFrom setCompletionStatus(String completionStatus) {
        this.completionStatus = completionStatus;
        return this;
    }
}
