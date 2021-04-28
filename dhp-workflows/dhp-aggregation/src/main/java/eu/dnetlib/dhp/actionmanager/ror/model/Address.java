
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class Address implements Serializable {

	private Float lat;
	private String stateCode;
	private Integer countryGeonamesId;
	private Float lng;
	private String state;
	private String city;
	private GeonamesCity geonamesCity;
	private String postcode;
	private Boolean primary;
	private String line;
	private final static long serialVersionUID = 2444635485253443195L;

	public Float getLat() {
		return lat;
	}

	public void setLat(final Float lat) {
		this.lat = lat;
	}

	public String getStateCode() {
		return stateCode;
	}

	public void setStateCode(final String stateCode) {
		this.stateCode = stateCode;
	}

	public Integer getCountryGeonamesId() {
		return countryGeonamesId;
	}

	public void setCountryGeonamesId(final Integer countryGeonamesId) {
		this.countryGeonamesId = countryGeonamesId;
	}

	public Float getLng() {
		return lng;
	}

	public void setLng(final Float lng) {
		this.lng = lng;
	}

	public String getState() {
		return state;
	}

	public void setState(final String state) {
		this.state = state;
	}

	public String getCity() {
		return city;
	}

	public void setCity(final String city) {
		this.city = city;
	}

	public GeonamesCity getGeonamesCity() {
		return geonamesCity;
	}

	public void setGeonamesCity(final GeonamesCity geonamesCity) {
		this.geonamesCity = geonamesCity;
	}

	public String getPostcode() {
		return postcode;
	}

	public void setPostcode(final String postcode) {
		this.postcode = postcode;
	}

	public Boolean getPrimary() {
		return primary;
	}

	public void setPrimary(final Boolean primary) {
		this.primary = primary;
	}

	public String getLine() {
		return line;
	}

	public void setLine(final String line) {
		this.line = line;
	}

}
