
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeonamesCity implements Serializable {

	@JsonProperty("geonames_admin1")
	private GeonamesAdmin geonamesAdmin1;

	@JsonProperty("geonames_admin2")
	private GeonamesAdmin geonamesAdmin2;

	@JsonProperty("city")
	private String city;

	@JsonProperty("id")
	private Integer id;

	@JsonProperty("nuts_level1")
	private NameAndCode nutsLevel1;

	@JsonProperty("nuts_level2")
	private NameAndCode nutsLevel2;

	@JsonProperty("nuts_level3")
	private NameAndCode nutsLevel3;

	@JsonProperty("")
	private License license;

	private final static long serialVersionUID = -8389480201526252955L;

	public NameAndCode getNutsLevel2() {
		return nutsLevel2;
	}

	public void setNutsLevel2(final NameAndCode nutsLevel2) {
		this.nutsLevel2 = nutsLevel2;
	}

	public GeonamesAdmin getGeonamesAdmin2() {
		return geonamesAdmin2;
	}

	public void setGeonamesAdmin2(final GeonamesAdmin geonamesAdmin2) {
		this.geonamesAdmin2 = geonamesAdmin2;
	}

	public GeonamesAdmin getGeonamesAdmin1() {
		return geonamesAdmin1;
	}

	public void setGeonamesAdmin1(final GeonamesAdmin geonamesAdmin1) {
		this.geonamesAdmin1 = geonamesAdmin1;
	}

	public String getCity() {
		return city;
	}

	public void setCity(final String city) {
		this.city = city;
	}

	public Integer getId() {
		return id;
	}

	public void setId(final Integer id) {
		this.id = id;
	}

	public NameAndCode getNutsLevel1() {
		return nutsLevel1;
	}

	public void setNutsLevel1(final NameAndCode nutsLevel1) {
		this.nutsLevel1 = nutsLevel1;
	}

	public NameAndCode getNutsLevel3() {
		return nutsLevel3;
	}

	public void setNutsLevel3(final NameAndCode nutsLevel3) {
		this.nutsLevel3 = nutsLevel3;
	}

	public License getLicense() {
		return license;
	}

	public void setLicense(final License license) {
		this.license = license;
	}

}
