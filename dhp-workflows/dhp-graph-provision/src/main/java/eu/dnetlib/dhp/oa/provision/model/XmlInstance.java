
package eu.dnetlib.dhp.oa.provision.model;

import java.util.Set;

import com.google.common.collect.Sets;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Serializable;

public class XmlInstance implements Serializable {

	public static final AccessRight UNKNOWN_ACCESS_RIGHT;
	public static final Qualifier UNKNOWN_REVIEW_LEVEL;

	static {
		UNKNOWN_ACCESS_RIGHT = new AccessRight();
		UNKNOWN_ACCESS_RIGHT.setClassid(ModelConstants.UNKNOWN);
		UNKNOWN_ACCESS_RIGHT.setClassname(ModelConstants.UNKNOWN);
		UNKNOWN_ACCESS_RIGHT.setSchemeid(ModelConstants.DNET_ACCESS_MODES);
		UNKNOWN_ACCESS_RIGHT.setSchemename(ModelConstants.DNET_ACCESS_MODES);

		UNKNOWN_REVIEW_LEVEL = new Qualifier();
		UNKNOWN_REVIEW_LEVEL.setClassid("0000");
		UNKNOWN_REVIEW_LEVEL.setClassname(ModelConstants.UNKNOWN);
		UNKNOWN_ACCESS_RIGHT.setSchemeid(ModelConstants.DNET_REVIEW_LEVELS);
		UNKNOWN_REVIEW_LEVEL.setSchemename(ModelConstants.DNET_REVIEW_LEVELS);
	}

	private String url;

	private AccessRight accessright;

	private Set<KeyValue> collectedfrom = Sets.newHashSet();

	private Set<KeyValue> hostedby = Sets.newHashSet();

	private Set<Qualifier> instancetype = Sets.newHashSet();

	private Set<String> license = Sets.newHashSet();

	// other research products specifc
	private Set<String> distributionlocation = Sets.newHashSet();

	private Set<StructuredProperty> pid = Sets.newHashSet();

	private Set<StructuredProperty> alternateIdentifier = Sets.newHashSet();

	private Set<String> dateofacceptance = Sets.newHashSet();

	// ( article | book ) processing charges. Defined here to cope with possible wrongly typed
	// results
	private String processingchargeamount;

	// currency - alphabetic code describe in ISO-4217. Defined here to cope with possible wrongly
	// typed results
	private String processingchargecurrency;

	private String fulltext;

	private Qualifier refereed; // peer-review status

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public AccessRight getAccessright() {
		return accessright;
	}

	public void setAccessright(AccessRight accessright) {
		this.accessright = accessright;
	}

	public Set<KeyValue> getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(Set<KeyValue> collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public Set<KeyValue> getHostedby() {
		return hostedby;
	}

	public void setHostedby(Set<KeyValue> hostedby) {
		this.hostedby = hostedby;
	}

	public Set<Qualifier> getInstancetype() {
		return instancetype;
	}

	public void setInstancetype(Set<Qualifier> instancetype) {
		this.instancetype = instancetype;
	}

	public Set<String> getLicense() {
		return license;
	}

	public void setLicense(Set<String> license) {
		this.license = license;
	}

	public Set<String> getDistributionlocation() {
		return distributionlocation;
	}

	public void setDistributionlocation(Set<String> distributionlocation) {
		this.distributionlocation = distributionlocation;
	}

	public Set<StructuredProperty> getPid() {
		return pid;
	}

	public void setPid(Set<StructuredProperty> pid) {
		this.pid = pid;
	}

	public Set<StructuredProperty> getAlternateIdentifier() {
		return alternateIdentifier;
	}

	public void setAlternateIdentifier(Set<StructuredProperty> alternateIdentifier) {
		this.alternateIdentifier = alternateIdentifier;
	}

	public Set<String> getDateofacceptance() {
		return dateofacceptance;
	}

	public void setDateofacceptance(Set<String> dateofacceptance) {
		this.dateofacceptance = dateofacceptance;
	}

	public String getProcessingchargeamount() {
		return processingchargeamount;
	}

	public void setProcessingchargeamount(String processingchargeamount) {
		this.processingchargeamount = processingchargeamount;
	}

	public String getProcessingchargecurrency() {
		return processingchargecurrency;
	}

	public void setProcessingchargecurrency(String processingchargecurrency) {
		this.processingchargecurrency = processingchargecurrency;
	}

	public Qualifier getRefereed() {
		return refereed;
	}

	public void setRefereed(Qualifier refereed) {
		this.refereed = refereed;
	}

	public String getFulltext() {
		return fulltext;
	}

	public void setFulltext(String fulltext) {
		this.fulltext = fulltext;
	}
}
