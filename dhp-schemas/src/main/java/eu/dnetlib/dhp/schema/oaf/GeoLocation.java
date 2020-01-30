package eu.dnetlib.dhp.schema.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class GeoLocation implements Serializable {

    private String point;

    private String box;

    private String place;

    public String getPoint() {
        return point;
    }

    public void setPoint(String point) {
        this.point = point;
    }

    public String getBox() {
        return box;
    }

    public void setBox(String box) {
        this.box = box;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    @JsonIgnore
    public boolean isBlank() {
        return StringUtils.isBlank(point) &&
                StringUtils.isBlank(box) &&
                StringUtils.isBlank(place);
    }

    public String toComparableString() {
        return isBlank()?"":String.format("%s::%s%s", point != null ? point.toLowerCase() : "", box != null ? box.toLowerCase() : "", place != null ? place.toLowerCase() : "");
    }

    @Override
    public int hashCode() {
        return toComparableString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        GeoLocation other = (GeoLocation) obj;

        return toComparableString()
                .equals(other.toComparableString());
    }
}
