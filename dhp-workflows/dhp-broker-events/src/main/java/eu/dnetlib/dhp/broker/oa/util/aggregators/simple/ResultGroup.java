
package eu.dnetlib.dhp.broker.oa.util.aggregators.simple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;

public class ResultGroup implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -3360828477088669296L;

	private List<OaBrokerMainEntity> data = new ArrayList<>();

	public List<OaBrokerMainEntity> getData() {
		return data;
	}

	public void setData(final List<OaBrokerMainEntity> data) {
		this.data = data;
	}

}
