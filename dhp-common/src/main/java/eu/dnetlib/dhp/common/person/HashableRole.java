package eu.dnetlib.dhp.common.person;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.HashableKeyValue;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.rel.beans.Role;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class HashableRole extends Role {
    public static HashableRole newInstance(String schema, String value, String text) {
        if(value == null && text == null)
            return null;

        final HashableRole role = new HashableRole();
        if(value != null && schema != null) {
            role.setValue(value);
            role.setSchema(schema);
        }
        if(text != null)
            role.setText(text);
        if(value != null && text == null && schema == null)
            role.setText(value);

        return role;
    }

    public static HashableRole newInstance(Role role) {
        HashableRole hrole = new HashableRole();
        hrole.setSchema(role.getSchema());
        hrole.setValue(role.getValue());
        hrole.setText(role.getText());
        return hrole;
    }

    public static Role toRole(HashableRole hrole) {
        Role role = new Role();
        role.setSchema(hrole.getSchema());
        role.setValue(hrole.getValue());
        role.setText(hrole.getText());
        return role;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 91)
                .append(getSchema())
                .append(getValue())
                .append(getText())
                .hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        final HashableRole rhs = (HashableRole) obj;
        return new EqualsBuilder()
                .append(getSchema(), rhs.getSchema())
                .append(getValue(), rhs.getValue())
                .append(getText(), rhs.getText())
                .isEquals();
    }

}
