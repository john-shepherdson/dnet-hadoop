package eu.dnetlib.dhp.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import eu.dnetlib.dhp.bulktag.community.SelectionConstraints;

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CommonConfigurationModel implements Serializable {
    private String zenodoCommunity;
    private List<String> subjects;
    private List<String> otherZenodoCommunities;
    private List<String> fos;
    private List<String> sdg;
    private SelectionConstraints advancedConstraints;
    private SelectionConstraints removeConstraints;

    public String getZenodoCommunity() {
        return zenodoCommunity;
    }

    public void setZenodoCommunity(String zenodoCommunity) {
        this.zenodoCommunity = zenodoCommunity;
    }

    public List<String> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<String> subjects) {
        this.subjects = subjects;
    }

    public List<String> getOtherZenodoCommunities() {
        return otherZenodoCommunities;
    }

    public void setOtherZenodoCommunities(List<String> otherZenodoCommunities) {
        this.otherZenodoCommunities = otherZenodoCommunities;
    }

    public List<String> getFos() {
        return fos;
    }

    public void setFos(List<String> fos) {
        this.fos = fos;
    }

    public List<String> getSdg() {
        return sdg;
    }

    public void setSdg(List<String> sdg) {
        this.sdg = sdg;
    }

    public SelectionConstraints getRemoveConstraints() {
        return removeConstraints;
    }

    public void setRemoveConstraints(SelectionConstraints removeConstraints) {
        this.removeConstraints = removeConstraints;
    }

    public SelectionConstraints getAdvancedConstraints() {
        return advancedConstraints;
    }

    public void setAdvancedConstraints(SelectionConstraints advancedConstraints) {
        this.advancedConstraints = advancedConstraints;
    }
}
