
package eu.dnetlib.dhp.sx.bio.pubmed;

/**
 * The type Pubmed subject.
 */
public class PMSubject {
	private String value;
	private String meshId;
	private String registryNumber;

	/**
	 * Instantiates a new Pm subject.
	 */
	public PMSubject() {
	}

	/**
	 * Instantiates a new Pm subject.
	 *
	 * @param value          the value
	 * @param meshId         the mesh id
	 * @param registryNumber the registry number
	 */
	public PMSubject(String value, String meshId, String registryNumber) {
		this.value = value;
		this.meshId = meshId;
		this.registryNumber = registryNumber;
	}

	/**
	 * Gets value.
	 *
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Sets value.
	 *
	 * @param value the value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Gets mesh id.
	 *
	 * @return the mesh id
	 */
	public String getMeshId() {
		return meshId;
	}

	/**
	 * Sets mesh id.
	 *
	 * @param meshId the mesh id
	 */
	public void setMeshId(String meshId) {
		this.meshId = meshId;
	}

	/**
	 * Gets registry number.
	 *
	 * @return the registry number
	 */
	public String getRegistryNumber() {
		return registryNumber;
	}

	/**
	 * Sets registry number.
	 *
	 * @param registryNumber the registry number
	 */
	public void setRegistryNumber(String registryNumber) {
		this.registryNumber = registryNumber;
	}
}
