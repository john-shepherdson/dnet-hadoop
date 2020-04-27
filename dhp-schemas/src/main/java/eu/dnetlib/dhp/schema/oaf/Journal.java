package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

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

  public void setName(String name) {
    this.name = name;
  }

  public String getIssnPrinted() {
    return issnPrinted;
  }

  public void setIssnPrinted(String issnPrinted) {
    this.issnPrinted = issnPrinted;
  }

  public String getIssnOnline() {
    return issnOnline;
  }

  public void setIssnOnline(String issnOnline) {
    this.issnOnline = issnOnline;
  }

  public String getIssnLinking() {
    return issnLinking;
  }

  public void setIssnLinking(String issnLinking) {
    this.issnLinking = issnLinking;
  }

  public String getEp() {
    return ep;
  }

  public void setEp(String ep) {
    this.ep = ep;
  }

  public String getIss() {
    return iss;
  }

  public void setIss(String iss) {
    this.iss = iss;
  }

  public String getSp() {
    return sp;
  }

  public void setSp(String sp) {
    this.sp = sp;
  }

  public String getVol() {
    return vol;
  }

  public void setVol(String vol) {
    this.vol = vol;
  }

  public String getEdition() {
    return edition;
  }

  public void setEdition(String edition) {
    this.edition = edition;
  }

  public String getConferenceplace() {
    return conferenceplace;
  }

  public void setConferenceplace(String conferenceplace) {
    this.conferenceplace = conferenceplace;
  }

  public String getConferencedate() {
    return conferencedate;
  }

  public void setConferencedate(String conferencedate) {
    this.conferencedate = conferencedate;
  }

  public DataInfo getDataInfo() {
    return dataInfo;
  }

  public void setDataInfo(DataInfo dataInfo) {
    this.dataInfo = dataInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Journal journal = (Journal) o;
    return Objects.equals(name, journal.name)
        && Objects.equals(issnPrinted, journal.issnPrinted)
        && Objects.equals(issnOnline, journal.issnOnline)
        && Objects.equals(issnLinking, journal.issnLinking)
        && Objects.equals(ep, journal.ep)
        && Objects.equals(iss, journal.iss)
        && Objects.equals(sp, journal.sp)
        && Objects.equals(vol, journal.vol)
        && Objects.equals(edition, journal.edition)
        && Objects.equals(conferenceplace, journal.conferenceplace)
        && Objects.equals(conferencedate, journal.conferencedate)
        && Objects.equals(dataInfo, journal.dataInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        issnPrinted,
        issnOnline,
        issnLinking,
        ep,
        iss,
        sp,
        vol,
        edition,
        conferenceplace,
        conferencedate,
        dataInfo);
  }
}
