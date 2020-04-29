package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class DataInfo implements Serializable {

  private Boolean invisible = false;
  private Boolean inferred;
  private Boolean deletedbyinference;
  private String trust;
  private String inferenceprovenance;
  private Qualifier provenanceaction;

  public Boolean getInvisible() {
    return invisible;
  }

  public void setInvisible(Boolean invisible) {
    this.invisible = invisible;
  }

  public Boolean getInferred() {
    return inferred;
  }

  public void setInferred(Boolean inferred) {
    this.inferred = inferred;
  }

  public Boolean getDeletedbyinference() {
    return deletedbyinference;
  }

  public void setDeletedbyinference(Boolean deletedbyinference) {
    this.deletedbyinference = deletedbyinference;
  }

  public String getTrust() {
    return trust;
  }

  public void setTrust(String trust) {
    this.trust = trust;
  }

  public String getInferenceprovenance() {
    return inferenceprovenance;
  }

  public void setInferenceprovenance(String inferenceprovenance) {
    this.inferenceprovenance = inferenceprovenance;
  }

  public Qualifier getProvenanceaction() {
    return provenanceaction;
  }

  public void setProvenanceaction(Qualifier provenanceaction) {
    this.provenanceaction = provenanceaction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataInfo dataInfo = (DataInfo) o;
    return Objects.equals(invisible, dataInfo.invisible)
        && Objects.equals(inferred, dataInfo.inferred)
        && Objects.equals(deletedbyinference, dataInfo.deletedbyinference)
        && Objects.equals(trust, dataInfo.trust)
        && Objects.equals(inferenceprovenance, dataInfo.inferenceprovenance)
        && Objects.equals(provenanceaction, dataInfo.provenanceaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        invisible, inferred, deletedbyinference, trust, inferenceprovenance, provenanceaction);
  }
}
