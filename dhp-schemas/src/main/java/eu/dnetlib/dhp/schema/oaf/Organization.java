package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Organization extends OafEntity implements Serializable {

    private Field<String> legalshortname;

    private Field<String> legalname;

    private List<Field<String>> alternativeNames;

    private Field<String> websiteurl;

    private Field<String> logourl;

    private Field<String> eclegalbody;

    private Field<String> eclegalperson;

    private Field<String> ecnonprofit;

    private Field<String> ecresearchorganization;

    private Field<String> echighereducation;

    private Field<String> ecinternationalorganizationeurinterests;

    private Field<String> ecinternationalorganization;

    private Field<String> ecenterprise;

    private Field<String> ecsmevalidated;

    private Field<String> ecnutscode;

    private Qualifier country;


    public Field<String> getLegalshortname() {
        return legalshortname;
    }

    public void setLegalshortname(Field<String> legalshortname) {
        this.legalshortname = legalshortname;
    }

    public Field<String> getLegalname() {
        return legalname;
    }

    public void setLegalname(Field<String> legalname) {
        this.legalname = legalname;
    }

    public List<Field<String>> getAlternativeNames() {
        return alternativeNames;
    }

    public void setAlternativeNames(List<Field<String>> alternativeNames) {
        this.alternativeNames = alternativeNames;
    }

    public Field<String> getWebsiteurl() {
        return websiteurl;
    }

    public void setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
    }

    public Field<String> getLogourl() {
        return logourl;
    }

    public void setLogourl(Field<String> logourl) {
        this.logourl = logourl;
    }

    public Field<String> getEclegalbody() {
        return eclegalbody;
    }

    public void setEclegalbody(Field<String> eclegalbody) {
        this.eclegalbody = eclegalbody;
    }

    public Field<String> getEclegalperson() {
        return eclegalperson;
    }

    public void setEclegalperson(Field<String> eclegalperson) {
        this.eclegalperson = eclegalperson;
    }

    public Field<String> getEcnonprofit() {
        return ecnonprofit;
    }

    public void setEcnonprofit(Field<String> ecnonprofit) {
        this.ecnonprofit = ecnonprofit;
    }

    public Field<String> getEcresearchorganization() {
        return ecresearchorganization;
    }

    public void setEcresearchorganization(Field<String> ecresearchorganization) {
        this.ecresearchorganization = ecresearchorganization;
    }

    public Field<String> getEchighereducation() {
        return echighereducation;
    }

    public void setEchighereducation(Field<String> echighereducation) {
        this.echighereducation = echighereducation;
    }

    public Field<String> getEcinternationalorganizationeurinterests() {
        return ecinternationalorganizationeurinterests;
    }

    public void setEcinternationalorganizationeurinterests(Field<String> ecinternationalorganizationeurinterests) {
        this.ecinternationalorganizationeurinterests = ecinternationalorganizationeurinterests;
    }

    public Field<String> getEcinternationalorganization() {
        return ecinternationalorganization;
    }

    public void setEcinternationalorganization(Field<String> ecinternationalorganization) {
        this.ecinternationalorganization = ecinternationalorganization;
    }

    public Field<String> getEcenterprise() {
        return ecenterprise;
    }

    public void setEcenterprise(Field<String> ecenterprise) {
        this.ecenterprise = ecenterprise;
    }

    public Field<String> getEcsmevalidated() {
        return ecsmevalidated;
    }

    public void setEcsmevalidated(Field<String> ecsmevalidated) {
        this.ecsmevalidated = ecsmevalidated;
    }

    public Field<String> getEcnutscode() {
        return ecnutscode;
    }

    public void setEcnutscode(Field<String> ecnutscode) {
        this.ecnutscode = ecnutscode;
    }

    public Qualifier getCountry() {
        return country;
    }

    public void setCountry(Qualifier country) {
        this.country = country;
    }
}
