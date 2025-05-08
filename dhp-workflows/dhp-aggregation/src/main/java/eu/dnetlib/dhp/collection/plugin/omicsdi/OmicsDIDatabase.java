package eu.dnetlib.dhp.collection.plugin.omicsdi;

import java.io.Serializable;

public class OmicsDIDatabase implements Serializable {

	private static final long serialVersionUID = 5553065831000797480L;

	private String databaseName;
	private String title;
	private String sourceUrl;
	private String imgAlt;
	private String repository;
	private String source;
	private String lastUpdated;
	private String orcidName;
	private String domain;
	private String description;
	private String image;
	private String icon;
	private String urlTemplate;
	private String[] accessionPrefix;

	public String getDatabaseName() {
		return this.databaseName;
	}

	public void setDatabaseName(final String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTitle() {
		return this.title;
	}

	public void setTitle(final String title) {
		this.title = title;
	}

	public String getSourceUrl() {
		return this.sourceUrl;
	}

	public void setSourceUrl(final String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getImgAlt() {
		return this.imgAlt;
	}

	public void setImgAlt(final String imgAlt) {
		this.imgAlt = imgAlt;
	}

	public String getRepository() {
		return this.repository;
	}

	public void setRepository(final String repository) {
		this.repository = repository;
	}

	public String getSource() {
		return this.source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getLastUpdated() {
		return this.lastUpdated;
	}

	public void setLastUpdated(final String lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getOrcidName() {
		return this.orcidName;
	}

	public void setOrcidName(final String orcidName) {
		this.orcidName = orcidName;
	}

	public String getDomain() {
		return this.domain;
	}

	public void setDomain(final String domain) {
		this.domain = domain;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(final String description) {
		this.description = description;
	}

	public String getImage() {
		return this.image;
	}

	public void setImage(final String image) {
		this.image = image;
	}

	public String getIcon() {
		return this.icon;
	}

	public void setIcon(final String icon) {
		this.icon = icon;
	}

	public String getUrlTemplate() {
		return this.urlTemplate;
	}

	public void setUrlTemplate(final String urlTemplate) {
		this.urlTemplate = urlTemplate;
	}

	public String[] getAccessionPrefix() {
		return this.accessionPrefix;
	}

	public void setAccessionPrefix(final String[] accessionPrefix) {
		this.accessionPrefix = accessionPrefix;
	}
}
