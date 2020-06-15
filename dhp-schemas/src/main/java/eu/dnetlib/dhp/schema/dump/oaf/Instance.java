
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

public class Instance implements Serializable {

	private String license;

	private AccessRight accessright;

	private String type;

	private KeyValue hostedby;

	private List<String> url;

	private KeyValue collectedfrom;

	private String publicationdate;// dateofacceptance;

	// ( article | book ) processing charges. Defined here to cope with possible wrongly typed
	// results
//	private Field<String> processingchargeamount;
//
//	// currency - alphabetic code describe in ISO-4217. Defined here to cope with possible wrongly
//	// typed results
//	private Field<String> processingchargecurrency;

	private String refereed; // peer-review status

	public String getLicense() {
		return license;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public AccessRight getAccessright() {
		return accessright;
	}

	public void setAccessright(AccessRight accessright) {
		this.accessright = accessright;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public KeyValue getHostedby() {
		return hostedby;
	}

	public void setHostedby(KeyValue hostedby) {
		this.hostedby = hostedby;
	}

	public List<String> getUrl() {
		return url;
	}

	public void setUrl(List<String> url) {
		this.url = url;
	}

	public KeyValue getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(KeyValue collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public String getPublicationdate() {
		return publicationdate;
	}

	public void setPublicationdate(String publicationdate) {
		this.publicationdate = publicationdate;
	}

	public String getRefereed() {
		return refereed;
	}

	public void setRefereed(String refereed) {
		this.refereed = refereed;
	}

}
