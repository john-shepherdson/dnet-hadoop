
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import eu.dnetlib.dhp.bulktag.community.SelectionConstraints;

/**
 * @author miriam.baglioni
 * @Date 06/10/23
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommunityModel implements Serializable {
	private String id;
	private String type;
	private String status;

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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
