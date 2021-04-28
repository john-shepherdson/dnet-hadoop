
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RorOrganization implements Serializable {

	private List<String> ipAddresses = new ArrayList<>();
	private List<String> aliases = new ArrayList<>();
	private List<String> acronyms = new ArrayList<>();
	private List<String> links = new ArrayList<>();
	private Country country;
	private String name;
	private String wikipediaUrl;
	private List<Address> addresses = new ArrayList<>();
	private List<String> types = new ArrayList<>();
	private Integer established;
	private List<Relationship> relationships = new ArrayList<>();
	private String emailAddress;
	private ExternalIds externalIds;
	private String id;
	private List<Label> labels = new ArrayList<>();
	private String status;
	private final static long serialVersionUID = -2658312087616043225L;

	public List<String> getIpAddresses() {
		return ipAddresses;
	}

	public void setIpAddresses(final List<String> ipAddresses) {
		this.ipAddresses = ipAddresses;
	}

	public List<String> getAliases() {
		return aliases;
	}

	public void setAliases(final List<String> aliases) {
		this.aliases = aliases;
	}

	public List<String> getAcronyms() {
		return acronyms;
	}

	public void setAcronyms(final List<String> acronyms) {
		this.acronyms = acronyms;
	}

	public List<String> getLinks() {
		return links;
	}

	public void setLinks(final List<String> links) {
		this.links = links;
	}

	public Country getCountry() {
		return country;
	}

	public void setCountry(final Country country) {
		this.country = country;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getWikipediaUrl() {
		return wikipediaUrl;
	}

	public void setWikipediaUrl(final String wikipediaUrl) {
		this.wikipediaUrl = wikipediaUrl;
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public void setAddresses(final List<Address> addresses) {
		this.addresses = addresses;
	}

	public List<String> getTypes() {
		return types;
	}

	public void setTypes(final List<String> types) {
		this.types = types;
	}

	public Integer getEstablished() {
		return established;
	}

	public void setEstablished(final Integer established) {
		this.established = established;
	}

	public List<Relationship> getRelationships() {
		return relationships;
	}

	public void setRelationships(final List<Relationship> relationships) {
		this.relationships = relationships;
	}

	public String getEmailAddress() {
		return emailAddress;
	}

	public void setEmailAddress(final String emailAddress) {
		this.emailAddress = emailAddress;
	}

	public ExternalIds getExternalIds() {
		return externalIds;
	}

	public void setExternalIds(final ExternalIds externalIds) {
		this.externalIds = externalIds;
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public List<Label> getLabels() {
		return labels;
	}

	public void setLabels(final List<Label> labels) {
		this.labels = labels;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(final String status) {
		this.status = status;
	}

}
