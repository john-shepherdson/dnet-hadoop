package eu.dnetlib.dhp.common.person;

import java.io.Serializable;

public class SerializationRoles implements Serializable {
    private String roleSchema;
    private String roleValue;
    private String roleName;

    public String getRoleSchema() {
        return roleSchema;
    }

    public void setRoleSchema(String roleSchema) {
        this.roleSchema = roleSchema;
    }

    public String getRoleValue() {
        return roleValue;
    }

    public void setRoleValue(String roleValue) {
        this.roleValue = roleValue;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
}
