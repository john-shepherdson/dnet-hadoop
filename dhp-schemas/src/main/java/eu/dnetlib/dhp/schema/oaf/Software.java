package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

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

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);

        if (!Software.class.isAssignableFrom(e.getClass())){
            return;
        }

        final Software s = (Software) e;
        documentationUrl = mergeLists(documentationUrl, s.getDocumentationUrl());

        license = mergeLists(license, s.getLicense());

        codeRepositoryUrl = s.getCodeRepositoryUrl()!= null && compareTrust(this, s)<0?s.getCodeRepositoryUrl():codeRepositoryUrl;

        programmingLanguage= s.getProgrammingLanguage()!= null && compareTrust(this, s)<0?s.getProgrammingLanguage():programmingLanguage;

        mergeOAFDataInfo(e);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Software software = (Software) o;
        return Objects.equals(documentationUrl, software.documentationUrl) &&
                Objects.equals(license, software.license) &&
                Objects.equals(codeRepositoryUrl, software.codeRepositoryUrl) &&
                Objects.equals(programmingLanguage, software.programmingLanguage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), documentationUrl, license, codeRepositoryUrl, programmingLanguage);
    }
}
