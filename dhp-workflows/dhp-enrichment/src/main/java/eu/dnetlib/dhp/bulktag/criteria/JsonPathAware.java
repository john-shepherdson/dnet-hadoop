package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

public interface JsonPathAware extends Serializable {
    void setJsonPath(String jsonpath);
}
