
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
	String group_id;
	String pid_list; // separator for type-pid: "###"; separator for pids: "@@@"
	Boolean ec_legalbody;
	Boolean ec_legalperson;
	Boolean ec_nonprofit;
	Boolean ec_researchorganization;
	Boolean ec_highereducation;
	Boolean ec_internationalorganizationeurinterests;
	Boolean ec_internationalorganization;
	Boolean ec_enterprise;
	Boolean ec_smevalidated;
	Boolean ec_nutscode;

	public OrgSimRel() {
	}

	public OrgSimRel(String local_id, String oa_original_id, String oa_name, String oa_acronym, String oa_country,
		String oa_url, String oa_collectedfrom, String group_id, String pid_list, Boolean ec_legalbody,
		Boolean ec_legalperson, Boolean ec_nonprofit, Boolean ec_researchorganization, Boolean ec_highereducation,
		Boolean ec_internationalorganizationeurinterests, Boolean ec_internationalorganization, Boolean ec_enterprise,
		Boolean ec_smevalidated, Boolean ec_nutscode) {
		this.local_id = local_id;
		this.oa_original_id = oa_original_id;
		this.oa_name = oa_name;
		this.oa_acronym = oa_acronym;
		this.oa_country = oa_country;
		this.oa_url = oa_url;
		this.oa_collectedfrom = oa_collectedfrom;
		this.group_id = group_id;
		this.pid_list = pid_list;
		this.ec_legalbody = ec_legalbody;
		this.ec_legalperson = ec_legalperson;
		this.ec_nonprofit = ec_nonprofit;
		this.ec_researchorganization = ec_researchorganization;
		this.ec_highereducation = ec_highereducation;
		this.ec_internationalorganizationeurinterests = ec_internationalorganizationeurinterests;
		this.ec_internationalorganization = ec_internationalorganization;
		this.ec_enterprise = ec_enterprise;
		this.ec_smevalidated = ec_smevalidated;
		this.ec_nutscode = ec_nutscode;
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

	public String getGroup_id() {
		return group_id;
	}

	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}

	public String getPid_list() {
		return pid_list;
	}

	public void setPid_list(String pid_list) {
		this.pid_list = pid_list;
	}

	public Boolean getEc_legalbody() {
		return ec_legalbody;
	}

	public void setEc_legalbody(Boolean ec_legalbody) {
		this.ec_legalbody = ec_legalbody;
	}

	public Boolean getEc_legalperson() {
		return ec_legalperson;
	}

	public void setEc_legalperson(Boolean ec_legalperson) {
		this.ec_legalperson = ec_legalperson;
	}

	public Boolean getEc_nonprofit() {
		return ec_nonprofit;
	}

	public void setEc_nonprofit(Boolean ec_nonprofit) {
		this.ec_nonprofit = ec_nonprofit;
	}

	public Boolean getEc_researchorganization() {
		return ec_researchorganization;
	}

	public void setEc_researchorganization(Boolean ec_researchorganization) {
		this.ec_researchorganization = ec_researchorganization;
	}

	public Boolean getEc_highereducation() {
		return ec_highereducation;
	}

	public void setEc_highereducation(Boolean ec_highereducation) {
		this.ec_highereducation = ec_highereducation;
	}

	public Boolean getEc_internationalorganizationeurinterests() {
		return ec_internationalorganizationeurinterests;
	}

	public void setEc_internationalorganizationeurinterests(Boolean ec_internationalorganizationeurinterests) {
		this.ec_internationalorganizationeurinterests = ec_internationalorganizationeurinterests;
	}

	public Boolean getEc_internationalorganization() {
		return ec_internationalorganization;
	}

	public void setEc_internationalorganization(Boolean ec_internationalorganization) {
		this.ec_internationalorganization = ec_internationalorganization;
	}

	public Boolean getEc_enterprise() {
		return ec_enterprise;
	}

	public void setEc_enterprise(Boolean ec_enterprise) {
		this.ec_enterprise = ec_enterprise;
	}

	public Boolean getEc_smevalidated() {
		return ec_smevalidated;
	}

	public void setEc_smevalidated(Boolean ec_smevalidated) {
		this.ec_smevalidated = ec_smevalidated;
	}

	public Boolean getEc_nutscode() {
		return ec_nutscode;
	}

	public void setEc_nutscode(Boolean ec_nutscode) {
		this.ec_nutscode = ec_nutscode;
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
			", group_id='" + group_id + '\'' +
			", pid_list='" + pid_list + '\'' +
			", ec_legalbody=" + ec_legalbody +
			", ec_legalperson=" + ec_legalperson +
			", ec_nonprofit=" + ec_nonprofit +
			", ec_researchorganization=" + ec_researchorganization +
			", ec_highereducation=" + ec_highereducation +
			", ec_internationalorganizationeurinterests=" + ec_internationalorganizationeurinterests +
			", ec_internationalorganization=" + ec_internationalorganization +
			", ec_enterprise=" + ec_enterprise +
			", ec_smevalidated=" + ec_smevalidated +
			", ec_nutscode=" + ec_nutscode +
			'}';
	}

}
