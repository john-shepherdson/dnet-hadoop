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

    public void setProvider(ScholixEntityId provider) {
        this.provider = provider;
    }

    public String getProvisionMode() {
        return provisionMode;
    }

    public void setProvisionMode(String provisionMode) {
        this.provisionMode = provisionMode;
    }

    public String getCompletionStatus() {
        return completionStatus;
    }

    public void setCompletionStatus(String completionStatus) {
        this.completionStatus = completionStatus;
    }
}
