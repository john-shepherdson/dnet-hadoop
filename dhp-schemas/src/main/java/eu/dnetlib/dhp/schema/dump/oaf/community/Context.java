
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.util.List;
import java.util.Objects;

import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.Qualifier;

/**
 * Reference to a relevant research infrastructure, initiative or community (RI/RC) among those collaborating with OpenAIRE. It extend eu.dnetlib.dhp.shema.dump.oaf.Qualifier with a parameter provenance of type List<eu.dnetlib.dhp.schema.dump.oaf.Provenance> to store the provenances of the association between the result and the RC/RI. The values for this element correspond to:
 *       - code: it corresponds to the id of the context in the result to be mapped. If the context id refers to a RC/RI and contains '::' only the part of the id before the first "::" will be used as value for code
 *       - label it corresponds to the label associated to the id. The information id taken from the profile of the RC/RI
 *       - provenance it is set only if the dataInfo associated to the contenxt element of the result to be dumped is not null. For each dataInfo one instance of type eu.dnetlib.dhp.schema.dump.oaf.Provenance is istanziated if the element datainfo.provenanceaction is not null. In this case
 *         - provenance corresponds to dataInfo.provenanceaction.classname
 *         - trust corresponds to dataInfo.trust
 */
public class Context extends Qualifier {
	private List<Provenance> provenance;

	public List<Provenance> getProvenance() {
		return provenance;
	}

	public void setProvenance(List<Provenance> provenance) {
		this.provenance = provenance;
	}

	@Override
	public int hashCode() {
		String provenance = new String();
		this.provenance.forEach(p -> provenance.concat(p.toString()));
		return Objects.hash(getCode(), getLabel(), provenance);
	}

}
