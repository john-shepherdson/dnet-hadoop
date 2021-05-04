
package eu.dnetlib.dhp.sx.ebi.model;

public class PMSubject {
	private String value;
	private String meshId;
	private String registryNumber;

	public PMSubject() {
	}

	public PMSubject(String value, String meshId, String registryNumber) {
		this.value = value;
		this.meshId = meshId;
		this.registryNumber = registryNumber;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getMeshId() {
		return meshId;
	}

	public void setMeshId(String meshId) {
		this.meshId = meshId;
	}

	public String getRegistryNumber() {
		return registryNumber;
	}

	public void setRegistryNumber(String registryNumber) {
		this.registryNumber = registryNumber;
	}
}
