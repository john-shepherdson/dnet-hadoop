
package eu.dnetlib.dhp.oa.dedup.model;

import java.io.Serializable;

public class OrgSimRel implements Serializable {

	String local_id;
	String oa_original_id;
	String oa_name;
	String oa_acronym;
	String oa_country;
	String oa_url;
	String oa_collectedfrom;

	public OrgSimRel() {
	}

	public OrgSimRel(String local_id, String oa_original_id, String oa_name, String oa_acronym, String oa_country,
		String oa_url, String oa_collectedfrom) {
		this.local_id = local_id;
		this.oa_original_id = oa_original_id;
		this.oa_name = oa_name;
		this.oa_acronym = oa_acronym;
		this.oa_country = oa_country;
		this.oa_url = oa_url;
		this.oa_collectedfrom = oa_collectedfrom;
	}

	public String getLocal_id() {
		return local_id;
	}

	public void setLocal_id(String local_id) {
		this.local_id = local_id;
	}

	public String getOa_original_id() {
		return oa_original_id;
	}

	public void setOa_original_id(String oa_original_id) {
		this.oa_original_id = oa_original_id;
	}

	public String getOa_name() {
		return oa_name;
	}

	public void setOa_name(String oa_name) {
		this.oa_name = oa_name;
	}

	public String getOa_acronym() {
		return oa_acronym;
	}

	public void setOa_acronym(String oa_acronym) {
		this.oa_acronym = oa_acronym;
	}

	public String getOa_country() {
		return oa_country;
	}

	public void setOa_country(String oa_country) {
		this.oa_country = oa_country;
	}

	public String getOa_url() {
		return oa_url;
	}

	public void setOa_url(String oa_url) {
		this.oa_url = oa_url;
	}

	public String getOa_collectedfrom() {
		return oa_collectedfrom;
	}

	public void setOa_collectedfrom(String oa_collectedfrom) {
		this.oa_collectedfrom = oa_collectedfrom;
	}

	@Override
	public String toString() {
		return "OrgSimRel{" +
			"local_id='" + local_id + '\'' +
			", oa_original_id='" + oa_original_id + '\'' +
			", oa_name='" + oa_name + '\'' +
			", oa_acronym='" + oa_acronym + '\'' +
			", oa_country='" + oa_country + '\'' +
			", oa_url='" + oa_url + '\'' +
			", oa_collectedfrom='" + oa_collectedfrom + '\'' +
			'}';
	}
}
