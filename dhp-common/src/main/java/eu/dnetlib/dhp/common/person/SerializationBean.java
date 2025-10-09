package eu.dnetlib.dhp.common.person;

import java.io.Serializable;
import java.util.List;

public class SerializationBean implements Serializable {
    private List<SerializationOrg> affs;
    private Boolean corresponding;
    private List<SerializationRoles> roles;



    public Boolean getCorresponding() {
        return corresponding;
    }

    public void setCorresponding(Boolean corresponding) {
        this.corresponding = corresponding;
    }

    public List<SerializationOrg> getAffs() {
        return affs;
    }

    public void setAffs(List<SerializationOrg> affs) {
        this.affs = affs;
    }

    public List<SerializationRoles> getRoles() {
        return roles;
    }

    public void setRoles(List<SerializationRoles> roles) {
        this.roles = roles;
    }
}
