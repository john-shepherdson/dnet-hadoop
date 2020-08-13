
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents the geolocation information. It has three parameters:
 * - point of type String to store the point information. It corresponds to eu.dnetlib.dhp.schema.oaf.GeoLocation point
 * - box ot type String to store the box information. It corresponds to eu.dnetlib.dhp.schema.oaf.GeoLocation box
 * - place of type String to store the place information. It corresponds to eu.dnetlib.dhp.schema.oaf.GeoLocation place
 */
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
		return StringUtils.isBlank(point) && StringUtils.isBlank(box) && StringUtils.isBlank(place);
	}

}
