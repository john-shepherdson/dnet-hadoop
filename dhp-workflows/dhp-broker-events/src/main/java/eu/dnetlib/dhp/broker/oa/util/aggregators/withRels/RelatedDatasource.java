
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;

public class RelatedDatasource implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 3015550240920424010L;

	private String source;
	private OaBrokerRelatedDatasource relDatasource;

	public RelatedDatasource() {
	}

	public RelatedDatasource(final String source, final OaBrokerRelatedDatasource relDatasource) {
		this.source = source;
		this.relDatasource = relDatasource;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public OaBrokerRelatedDatasource getRelDatasource() {
		return relDatasource;
	}

	public void setRelDatasource(final OaBrokerRelatedDatasource relDatasource) {
		this.relDatasource = relDatasource;
	}

}
