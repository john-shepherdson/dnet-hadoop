
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.orcid.Contributor;
import eu.dnetlib.dhp.schema.orcid.ExternalId;
import eu.dnetlib.dhp.schema.orcid.OrcidData;
import eu.dnetlib.dhp.schema.orcid.PublicationDate;

/**
 * This class models the data that are retrieved from orcid publication
 */

public class WorkDetail implements Serializable {

	private String oid;
	private String id;
	private String sourceName;
	private String type;
	private List<String> titles;
	private List<String> urls;
	List<ExternalId> extIds;
	List<PublicationDate> publicationDates;
	List<Contributor> contributors;

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	private String errorCode;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTitles() {
		return titles;
	}

	public void setTitles(List<String> titles) {
		this.titles = titles;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<String> getUrls() {
		return urls;
	}

	public void setUrls(List<String> urls) {
		this.urls = urls;
	}

	public List<ExternalId> getExtIds() {
		return extIds;
	}

	public void setExtIds(List<ExternalId> extIds) {
		this.extIds = extIds;
	}

	public List<PublicationDate> getPublicationDates() {
		return publicationDates;
	}

	public void setPublicationDates(List<PublicationDate> publicationDates) {
		this.publicationDates = publicationDates;
	}

	public List<Contributor> getContributors() {
		return contributors;
	}

	public void setContributors(List<Contributor> contributors) {
		this.contributors = contributors;
	}
}
