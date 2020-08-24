
package eu.dnetlib.dhp.schema.dump.oaf.community;

import eu.dnetlib.dhp.schema.dump.oaf.Instance;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;

/**
 * It extends eu.dnetlib.dhp.dump.oaf.Instance with values related to the community dump. In the Result dump this
 * information is not present because it is dumped as a set of relations between the result and the datasource. -
 * hostedby of type eu.dnetlib.dhp.schema.dump.oaf.KeyValue to store the information about the source from which the
 * instance can be viewed or downloaded. It is mapped against the hostedby parameter of the instance to be dumped and -
 * key corresponds to hostedby.key - value corresponds to hostedby.value - collectedfrom of type
 * eu.dnetlib.dhp.schema.dump.oaf.KeyValue to store the information about the source from which the instance has been
 * collected. It is mapped against the collectedfrom parameter of the instance to be dumped and - key corresponds to
 * collectedfrom.key - value corresponds to collectedfrom.value
 */
public class CommunityInstance extends Instance {
	private KeyValue hostedby;
	private KeyValue collectedfrom;

	public KeyValue getHostedby() {
		return hostedby;
	}

	public void setHostedby(KeyValue hostedby) {
		this.hostedby = hostedby;
	}

	public KeyValue getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(KeyValue collectedfrom) {
		this.collectedfrom = collectedfrom;
	}
}
