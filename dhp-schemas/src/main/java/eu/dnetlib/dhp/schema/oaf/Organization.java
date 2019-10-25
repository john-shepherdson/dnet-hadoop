package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Organization extends OafEntity<Organization> implements Serializable {

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

    public Organization setLegalshortname(Field<String> legalshortname) {
        this.legalshortname = legalshortname;
        return this;
    }

    public Field<String> getLegalname() {
        return legalname;
    }

    public Organization setLegalname(Field<String> legalname) {
        this.legalname = legalname;
        return this;
    }

    public List<Field<String>> getAlternativeNames() {
        return alternativeNames;
    }

    public Organization setAlternativeNames(List<Field<String>> alternativeNames) {
        this.alternativeNames = alternativeNames;
        return this;
    }

    public Field<String> getWebsiteurl() {
        return websiteurl;
    }

    public Organization setWebsiteurl(Field<String> websiteurl) {
        this.websiteurl = websiteurl;
        return this;
    }

    public Field<String> getLogourl() {
        return logourl;
    }

    public Organization setLogourl(Field<String> logourl) {
        this.logourl = logourl;
        return this;
    }

    public Field<String> getEclegalbody() {
        return eclegalbody;
    }

    public Organization setEclegalbody(Field<String> eclegalbody) {
        this.eclegalbody = eclegalbody;
        return this;
    }

    public Field<String> getEclegalperson() {
        return eclegalperson;
    }

    public Organization setEclegalperson(Field<String> eclegalperson) {
        this.eclegalperson = eclegalperson;
        return this;
    }

    public Field<String> getEcnonprofit() {
        return ecnonprofit;
    }

    public Organization setEcnonprofit(Field<String> ecnonprofit) {
        this.ecnonprofit = ecnonprofit;
        return this;
    }

    public Field<String> getEcresearchorganization() {
        return ecresearchorganization;
    }

    public Organization setEcresearchorganization(Field<String> ecresearchorganization) {
        this.ecresearchorganization = ecresearchorganization;
        return this;
    }

    public Field<String> getEchighereducation() {
        return echighereducation;
    }

    public Organization setEchighereducation(Field<String> echighereducation) {
        this.echighereducation = echighereducation;
        return this;
    }

    public Field<String> getEcinternationalorganizationeurinterests() {
        return ecinternationalorganizationeurinterests;
    }

    public Organization setEcinternationalorganizationeurinterests(Field<String> ecinternationalorganizationeurinterests) {
        this.ecinternationalorganizationeurinterests = ecinternationalorganizationeurinterests;
        return this;
    }

    public Field<String> getEcinternationalorganization() {
        return ecinternationalorganization;
    }

    public Organization setEcinternationalorganization(Field<String> ecinternationalorganization) {
        this.ecinternationalorganization = ecinternationalorganization;
        return this;
    }

    public Field<String> getEcenterprise() {
        return ecenterprise;
    }

    public Organization setEcenterprise(Field<String> ecenterprise) {
        this.ecenterprise = ecenterprise;
        return this;
    }

    public Field<String> getEcsmevalidated() {
        return ecsmevalidated;
    }

    public Organization setEcsmevalidated(Field<String> ecsmevalidated) {
        this.ecsmevalidated = ecsmevalidated;
        return this;
    }

    public Field<String> getEcnutscode() {
        return ecnutscode;
    }

    public Organization setEcnutscode(Field<String> ecnutscode) {
        this.ecnutscode = ecnutscode;
        return this;
    }

    public Qualifier getCountry() {
        return country;
    }

    public Organization setCountry(Qualifier country) {
        this.country = country;
        return this;
    }

    @Override
    protected Organization self() {
        return this;
    }
}
