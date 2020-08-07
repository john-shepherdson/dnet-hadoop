
package eu.dnetlib.dhp.common.api.zenodo;

import java.io.Serializable;
import java.util.List;

public class Metadata implements Serializable {

	private String access_right;
	private List<Community> communities;
	private List<Creator> creators;
	private String description;
	private String doi;
	private List<Grant> grants;
	private List<String> keywords;
	private String language;
	private String license;
	private PrereserveDoi prereserve_doi;
	private String publication_date;
	private List<String> references;
	private List<RelatedIdentifier> related_identifiers;
	private String title;
	private String upload_type;
	private String version;

	public String getUpload_type() {
		return upload_type;
	}

	public void setUpload_type(String upload_type) {
		this.upload_type = upload_type;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getAccess_right() {
		return access_right;
	}

	public void setAccess_right(String access_right) {
		this.access_right = access_right;
	}

	public List<Community> getCommunities() {
		return communities;
	}

	public void setCommunities(List<Community> communities) {
		this.communities = communities;
	}

	public List<Creator> getCreators() {
		return creators;
	}

	public void setCreators(List<Creator> creators) {
		this.creators = creators;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public List<Grant> getGrants() {
		return grants;
	}

	public void setGrants(List<Grant> grants) {
		this.grants = grants;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getLicense() {
		return license;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public PrereserveDoi getPrereserve_doi() {
		return prereserve_doi;
	}

	public void setPrereserve_doi(PrereserveDoi prereserve_doi) {
		this.prereserve_doi = prereserve_doi;
	}

	public String getPublication_date() {
		return publication_date;
	}

	public void setPublication_date(String publication_date) {
		this.publication_date = publication_date;
	}

	public List<String> getReferences() {
		return references;
	}

	public void setReferences(List<String> references) {
		this.references = references;
	}

	public List<RelatedIdentifier> getRelated_identifiers() {
		return related_identifiers;
	}

	public void setRelated_identifiers(List<RelatedIdentifier> related_identifiers) {
		this.related_identifiers = related_identifiers;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
}
