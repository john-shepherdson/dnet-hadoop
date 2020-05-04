
package eu.dnetlib.dhp.provision;

import java.io.Serializable;

/** This class models the information of related items */
public class RelatedItemInfo implements Serializable {

	private String source;

	private long relatedDataset = 0;

	private long relatedPublication = 0;

	private long relatedUnknown = 0;

	public RelatedItemInfo() {
	}

	public RelatedItemInfo(
		String source, long relatedDataset, long relatedPublication, long relatedUnknown) {
		this.source = source;
		this.relatedDataset = relatedDataset;
		this.relatedPublication = relatedPublication;
		this.relatedUnknown = relatedUnknown;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public long getRelatedDataset() {
		return relatedDataset;
	}

	public void setRelatedDataset(long relatedDataset) {
		this.relatedDataset = relatedDataset;
	}

	public long getRelatedPublication() {
		return relatedPublication;
	}

	public void setRelatedPublication(long relatedPublication) {
		this.relatedPublication = relatedPublication;
	}

	public long getRelatedUnknown() {
		return relatedUnknown;
	}

	public void setRelatedUnknown(int relatedUnknown) {
		this.relatedUnknown = relatedUnknown;
	}
}
