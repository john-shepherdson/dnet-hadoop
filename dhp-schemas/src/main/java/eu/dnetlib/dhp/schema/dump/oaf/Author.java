
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

/**
 * Used to represent the generic author of the result. It has six parameters:
 * - name of type String to store the given name of the author. The value for this parameter corresponds
 *   to eu.dnetlib.dhp.schema.oaf.Author name
 * - surname of type String to store the family name of the author. The value for this parameter corresponds to
 *   eu.dnetlib.dhp.schema.oaf.Author surname
 * - fullname of type String to store the fullname of the author. The value for this parameter corresponds to
 *   eu.dnetlib.dhp.schema.oaf.Author fullname
 * - rank of type Integer to store the rank on the author in the result's authors list. The value for this parameter
 *   corresponds to eu.dnetlib.dhp.schema.oaf.Author rank
 * - pid of type eu.dnetlib.dhp.schema.dump.oaf.Pid to store the persistent identifier for the author. For the moment
 *   only ORCID identifiers will be dumped.
 *   - The id element is instantiated by using the following values in the eu.dnetlib.dhp.schema.oaf.Result pid:
 *          * Qualifier.classid for scheme
 *          * value  for value
 *   - The provenance element is instantiated only if the dataInfo is set for the pid in the result to be dumped. The provenance element is instantiated by using the following values in the eu.dnetlib.dhp.schema.oaf.Result pid:
 *          * dataInfo.provenanceaction.classname for provenance
 *          * dataInfo.trust for trust
 */
public class Author implements Serializable {

	private String fullname;

	private String name;

	private String surname;

	private Integer rank;

	private Pid pid;

	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public Integer getRank() {
		return rank;
	}

	public void setRank(Integer rank) {
		this.rank = rank;
	}

	public Pid getPid() {
		return pid;
	}

	public void setPid(Pid pid) {
		this.pid = pid;
	}

}
