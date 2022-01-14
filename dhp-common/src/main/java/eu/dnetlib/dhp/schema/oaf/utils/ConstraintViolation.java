package eu.dnetlib.dhp.schema.oaf.utils;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ConstraintViolation implements Serializable {

    private Map<String, String> properties = new HashMap<>();

    public ConstraintViolation() {}

    /*
    @JsonCreator
    public ConstraintViolation(
            @JsonProperty("path") final String path,
            @JsonProperty("message") final String message) {
        this.properties.put(path, message);
    }
    */

    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void add(String key, String value) {
        properties.put(key, value);
    }

}
