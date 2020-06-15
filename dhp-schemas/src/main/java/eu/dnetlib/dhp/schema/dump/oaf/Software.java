
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class Software extends Result implements Serializable {

	private List<String> documentationUrl;

	private String codeRepositoryUrl;

	private String programmingLanguage;

	public Software() {
		setType(ModelConstants.SOFTWARE_DEFAULT_RESULTTYPE.getClassname());
	}

	public List<String> getDocumentationUrl() {
		return documentationUrl;
	}

	public void setDocumentationUrl(List<String> documentationUrl) {
		this.documentationUrl = documentationUrl;
	}

	public String getCodeRepositoryUrl() {
		return codeRepositoryUrl;
	}

	public void setCodeRepositoryUrl(String codeRepositoryUrl) {
		this.codeRepositoryUrl = codeRepositoryUrl;
	}

	public String getProgrammingLanguage() {
		return programmingLanguage;
	}

	public void setProgrammingLanguage(String programmingLanguage) {
		this.programmingLanguage = programmingLanguage;
	}

}
