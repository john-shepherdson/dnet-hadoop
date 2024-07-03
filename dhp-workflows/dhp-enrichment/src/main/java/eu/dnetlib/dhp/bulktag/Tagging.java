package eu.dnetlib.dhp.bulktag;

import java.io.Serializable;
import eu.dnetlib.dhp.schema.oaf.Result;

public  class Tagging <R extends Result> implements Serializable {
    private String tag;
    private R result;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public R getResult() {
        return result;
    }

    public void setResult(R result) {
        this.result = result;
    }

    public static <R extends Result> Tagging newInstance(R result, String tag){
        Tagging t = new Tagging<>();
        t.result = result;
        t.tag = tag;
        return t;
    }
}
