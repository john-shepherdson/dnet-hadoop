
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Software;

public class RelatedSoftware implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7573383356943300157L;
	private String source;
	private String relType;
	private Software relSoftware;

	public RelatedSoftware() {
	}

	public RelatedSoftware(final String source, final String relType, final Software relSoftware) {
		this.source = source;
		this.relType = relType;
		this.relSoftware = relSoftware;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getRelType() {
		return relType;
	}

	public void setRelType(final String relType) {
		this.relType = relType;
	}

	public Software getRelSoftware() {
		return relSoftware;
	}

	public void setRelSoftware(final Software relSoftware) {
		this.relSoftware = relSoftware;
	}

}
