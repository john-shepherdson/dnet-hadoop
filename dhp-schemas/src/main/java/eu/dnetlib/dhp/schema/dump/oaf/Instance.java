
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the manifestations (i.e. different versions) of the result. For example: the pre-print and the published
 * versions are two manifestations of the same research result. It has the following parameters:
 * - license  of type String to store the license applied to the instance. It corresponds to the value of the licence in
 *   the instance to be dumped
 * - accessright of type eu.dnetlib.dhp.schema.dump.oaf.AccessRight to store the accessright of the instance.
 * - type of type String to store the type of the instance as defined in the corresponding dnet vocabulary
 *   (dnet:pubication_resource). It corresponds to the instancetype.classname of the instance to be mapped
 * - hostedby of type eu.dnetlib.dhp.schema.dump.oaf.KeyValue to store the information about the source from which the
 *   instance can be viewed or downloaded. It is mapped against the hostedby parameter of the instance to be dumped and
 *   - key corresponds to hostedby.key
 *   - value corresponds to hostedby.value
 * - url of type List<String> list of locations where the instance is accessible. It corresponds to url of the instance
 *   to be dumped
 * - collectedfrom of type eu.dnetlib.dhp.schema.dump.oaf.KeyValue to store the information about the source from which
 *   the instance has been collected. It is mapped against the collectedfrom parameter of the instance to be dumped and
 *   - key corresponds to collectedfrom.key
 *   - value corresponds to collectedfrom.value
 * - publicationdate of type String to store the publication date of the instance ;// dateofacceptance;
 * - refereed of type String to store information abour tthe review status of the instance. Possible values are
 *   'Unknown', 'nonPeerReviewed', 'peerReviewed'. It corresponds to refereed.classname of the instance to be dumped
 */
public class Instance implements Serializable {

	private String license;

	private AccessRight accessright;

	private String type;

	private KeyValue hostedby;

	private List<String> url;

	private KeyValue collectedfrom;

	private String publicationdate;// dateofacceptance;

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
