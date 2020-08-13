
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.Objects;

/**
 * To store information about the conference or journal where the result has been presented or published.
 * It contains eleven parameters:
 * - name of type String to store the name of the journal or conference. It corresponds to the parameter name of
 *   eu.dnetlib.dhp.schema.oaf.Journal
 * - issnPrinted ot type String to store the journal printed issn. It corresponds to the parameter issnPrinted of
 *   eu.dnetlib.dhp.schema.oaf.Journal
 * - issnOnline of type String to store the journal online issn. It corresponds to the parameter issnOnline of
 *   eu.dnetlib.dhp.schema.oaf.Journal
 * - issnLinking of type String to store the journal linking issn. It corresponds to the parameter issnLinking of
 *   eu.dnetlib.dhp.schema.oaf.Journal
 * - ep of type String to store the end page. It corresponds to the parameter ep of eu.dnetlib.dhp.schema.oaf.Journal
 * - iss of type String to store the journal issue. It corresponds to the parameter iss of eu.dnetlib.dhp.schema.oaf.Journal
 * - sp of type String to store the start page. It corresponds to the parameter sp of eu.dnetlib.dhp.schema.oaf.Journal
 * - vol of type String to store the Volume. It corresponds to the parameter vol of eu.dnetlib.dhp.schema.oaf.Journal
 * - edition of type String to store the edition of the journal or conference proceeding. It corresponds to the
 *   parameter edition of eu.dnetlib.dhp.schema.oaf.Journal
 * - conferenceplace of type String to store the place of the conference. It corresponds to the parameter
 *   conferenceplace of eu.dnetlib.dhp.schema.oaf.Journal
 * - conferencedate of type String to store the date of the conference. It corresponds to the parameter conferencedate
 *   of eu.dnetlib.dhp.schema.oaf.Journal
 */
public class Container implements Serializable {

	private String name;

	private String issnPrinted;

	private String issnOnline;

	private String issnLinking;

	private String ep;

	private String iss;

	private String sp;

	private String vol;

	private String edition;

	private String conferenceplace;

	private String conferencedate;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIssnPrinted() {
		return issnPrinted;
	}

	public void setIssnPrinted(String issnPrinted) {
		this.issnPrinted = issnPrinted;
	}

	public String getIssnOnline() {
		return issnOnline;
	}

	public void setIssnOnline(String issnOnline) {
		this.issnOnline = issnOnline;
	}

	public String getIssnLinking() {
		return issnLinking;
	}

	public void setIssnLinking(String issnLinking) {
		this.issnLinking = issnLinking;
	}

	public String getEp() {
		return ep;
	}

	public void setEp(String ep) {
		this.ep = ep;
	}

	public String getIss() {
		return iss;
	}

	public void setIss(String iss) {
		this.iss = iss;
	}

	public String getSp() {
		return sp;
	}

	public void setSp(String sp) {
		this.sp = sp;
	}

	public String getVol() {
		return vol;
	}

	public void setVol(String vol) {
		this.vol = vol;
	}

	public String getEdition() {
		return edition;
	}

	public void setEdition(String edition) {
		this.edition = edition;
	}

	public String getConferenceplace() {
		return conferenceplace;
	}

	public void setConferenceplace(String conferenceplace) {
		this.conferenceplace = conferenceplace;
	}

	public String getConferencedate() {
		return conferencedate;
	}

	public void setConferencedate(String conferencedate) {
		this.conferencedate = conferencedate;
	}

}
