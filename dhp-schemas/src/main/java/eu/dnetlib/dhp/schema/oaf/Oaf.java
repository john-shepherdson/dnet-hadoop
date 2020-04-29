package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public abstract class Oaf implements Serializable {

  protected List<KeyValue> collectedfrom;

  private DataInfo dataInfo;

  private Long lastupdatetimestamp;

  public List<KeyValue> getCollectedfrom() {
    return collectedfrom;
  }

  public void setCollectedfrom(List<KeyValue> collectedfrom) {
    this.collectedfrom = collectedfrom;
  }

  public DataInfo getDataInfo() {
    return dataInfo;
  }

  public void setDataInfo(DataInfo dataInfo) {
    this.dataInfo = dataInfo;
  }

  public Long getLastupdatetimestamp() {
    return lastupdatetimestamp;
  }

  public void setLastupdatetimestamp(Long lastupdatetimestamp) {
    this.lastupdatetimestamp = lastupdatetimestamp;
  }

  public void mergeOAFDataInfo(Oaf e) {
    if (e.getDataInfo() != null && compareTrust(this, e) < 0) dataInfo = e.getDataInfo();
  }

  protected String extractTrust(Oaf e) {
    if (e == null || e.getDataInfo() == null || e.getDataInfo().getTrust() == null) return "0.0";
    return e.getDataInfo().getTrust();
  }

  protected int compareTrust(Oaf a, Oaf b) {
    return extractTrust(a).compareTo(extractTrust(b));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Oaf oaf = (Oaf) o;
    return Objects.equals(dataInfo, oaf.dataInfo)
        && Objects.equals(lastupdatetimestamp, oaf.lastupdatetimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataInfo, lastupdatetimestamp);
  }
}
