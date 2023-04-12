package eu.dnetlib.dhp.sx.graph.scholix;

import java.util.List;

public class ScholixFlat {
    private String identifier;
    private String relationType;
    private String sourceId;
    private String sourceType;
    private String sourceSubType;
    private List<String> sourcePid;
    private List<String> sourcePidType;
    private List<String> sourcePublisher;
    private String targetId;
    private String targetType;
    private String targetSubType;
    private List<String> targetPid;
    private List<String> targetPidType;
    private List<String> targetPublisher;
    private List<String> linkProviders;
    private String publicationDate;
    private String blob;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getRelationType() {
        return relationType;
    }

    public void setRelationType(String relationType) {
        this.relationType = relationType;
    }
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceSubType() {
        return sourceSubType;
    }

    public void setSourceSubType(String sourceSubType) {
        this.sourceSubType = sourceSubType;
    }

    public List<String> getSourcePid() {
        return sourcePid;
    }

    public void setSourcePid(List<String> sourcePid) {
        this.sourcePid = sourcePid;
    }

    public List<String> getSourcePidType() {
        return sourcePidType;
    }

    public void setSourcePidType(List<String> sourcePidType) {
        this.sourcePidType = sourcePidType;
    }

    public List<String> getSourcePublisher() {
        return sourcePublisher;
    }

    public void setSourcePublisher(List<String> sourcePublisher) {
        this.sourcePublisher = sourcePublisher;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetSubType() {
        return targetSubType;
    }

    public void setTargetSubType(String targetSubType) {
        this.targetSubType = targetSubType;
    }

    public List<String> getTargetPid() {
        return targetPid;
    }

    public void setTargetPid(List<String> targetPid) {
        this.targetPid = targetPid;
    }

    public List<String> getTargetPidType() {
        return targetPidType;
    }

    public void setTargetPidType(List<String> targetPidType) {
        this.targetPidType = targetPidType;
    }

    public List<String> getTargetPublisher() {
        return targetPublisher;
    }

    public void setTargetPublisher(List<String> targetPublisher) {
        this.targetPublisher = targetPublisher;
    }

    public List<String> getLinkProviders() {
        return linkProviders;
    }

    public void setLinkProviders(List<String> linkProviders) {
        this.linkProviders = linkProviders;
    }

    public String getPublicationDate() {
        return publicationDate;
    }

    public void setPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
    }

    public String getBlob() {
        return blob;
    }

    public void setBlob(String blob) {
        this.blob = blob;
    }
}
