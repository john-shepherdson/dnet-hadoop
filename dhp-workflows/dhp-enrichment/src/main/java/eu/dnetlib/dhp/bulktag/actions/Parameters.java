package eu.dnetlib.dhp.bulktag.actions;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 22/01/24
 */
public class Parameters implements Serializable {
    private String paramName;
    private String paramValue;

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getParamValue() {
        return paramValue;
    }

    public void setParamValue(String paramValue) {
        this.paramValue = paramValue;
    }
}
