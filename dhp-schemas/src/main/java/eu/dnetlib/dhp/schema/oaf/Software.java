package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Software extends Result implements Serializable {

    private List<Field<String>> documentationUrl;

    private List<StructuredProperty> license;

    private Field<String> codeRepositoryUrl;

    private Qualifier programmingLanguage;

    public List<Field<String>> getDocumentationUrl() {
        return documentationUrl;
    }

    public void setDocumentationUrl(List<Field<String>> documentationUrl) {
        this.documentationUrl = documentationUrl;
    }

    public List<StructuredProperty> getLicense() {
        return license;
    }

    public void setLicense(List<StructuredProperty> license) {
        this.license = license;
    }

    public Field<String> getCodeRepositoryUrl() {
        return codeRepositoryUrl;
    }

    public void setCodeRepositoryUrl(Field<String> codeRepositoryUrl) {
        this.codeRepositoryUrl = codeRepositoryUrl;
    }

    public Qualifier getProgrammingLanguage() {
        return programmingLanguage;
    }

    public void setProgrammingLanguage(Qualifier programmingLanguage) {
        this.programmingLanguage = programmingLanguage;
    }
}
