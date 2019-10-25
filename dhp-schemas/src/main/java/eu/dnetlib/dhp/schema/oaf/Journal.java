package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Journal implements Serializable {

    private String name;

    private String issnPrinted;

    private String issnOnline;

    private String issnLinking;

    private String ep;

    private String iss;

    private String sp;

    private String vol;

    private String edition;

    private String conferenceplace;

    private String conferencedate;

    private DataInfo dataInfo;

    public String getName() {
        return name;
    }

    public Journal setName(String name) {
        this.name = name;
        return this;
    }

    public String getIssnPrinted() {
        return issnPrinted;
    }

    public Journal setIssnPrinted(String issnPrinted) {
        this.issnPrinted = issnPrinted;
        return this;
    }

    public String getIssnOnline() {
        return issnOnline;
    }

    public Journal setIssnOnline(String issnOnline) {
        this.issnOnline = issnOnline;
        return this;
    }

    public String getIssnLinking() {
        return issnLinking;
    }

    public Journal setIssnLinking(String issnLinking) {
        this.issnLinking = issnLinking;
        return this;
    }

    public String getEp() {
        return ep;
    }

    public Journal setEp(String ep) {
        this.ep = ep;
        return this;
    }

    public String getIss() {
        return iss;
    }

    public Journal setIss(String iss) {
        this.iss = iss;
        return this;
    }

    public String getSp() {
        return sp;
    }

    public Journal setSp(String sp) {
        this.sp = sp;
        return this;
    }

    public String getVol() {
        return vol;
    }

    public Journal setVol(String vol) {
        this.vol = vol;
        return this;
    }

    public String getEdition() {
        return edition;
    }

    public Journal setEdition(String edition) {
        this.edition = edition;
        return this;
    }

    public String getConferenceplace() {
        return conferenceplace;
    }

    public Journal setConferenceplace(String conferenceplace) {
        this.conferenceplace = conferenceplace;
        return this;
    }

    public String getConferencedate() {
        return conferencedate;
    }

    public Journal setConferencedate(String conferencedate) {
        this.conferencedate = conferencedate;
        return this;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public Journal setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
