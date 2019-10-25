package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class GeoLocation implements Serializable {

    private String point;

    private String box;

    private String place;

    public String getPoint() {
        return point;
    }

    public GeoLocation setPoint(String point) {
        this.point = point;
        return this;
    }

    public String getBox() {
        return box;
    }

    public GeoLocation setBox(String box) {
        this.box = box;
        return this;
    }

    public String getPlace() {
        return place;
    }

    public GeoLocation setPlace(String place) {
        this.place = place;
        return this;
    }
}
