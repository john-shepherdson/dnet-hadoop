package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Software extends Result<Software> implements Serializable {

    private List<Field<String>> documentationUrl;

    private List<StructuredProperty> license;

    private Field<String> codeRepositoryUrl;

    private Qualifier programmingLanguage;

    public List<Field<String>> getDocumentationUrl() {
        return documentationUrl;
    }

    public Software setDocumentationUrl(List<Field<String>> documentationUrl) {
        this.documentationUrl = documentationUrl;
        return this;
    }

    public List<StructuredProperty> getLicense() {
        return license;
    }

    public Software setLicense(List<StructuredProperty> license) {
        this.license = license;
        return this;
    }

    public Field<String> getCodeRepositoryUrl() {
        return codeRepositoryUrl;
    }

    public Software setCodeRepositoryUrl(Field<String> codeRepositoryUrl) {
        this.codeRepositoryUrl = codeRepositoryUrl;
        return this;
    }

    public Qualifier getProgrammingLanguage() {
        return programmingLanguage;
    }

    public Software setProgrammingLanguage(Qualifier programmingLanguage) {
        this.programmingLanguage = programmingLanguage;
        return this;
    }

    @Override
    protected Software self() {
        return this;
    }
}
