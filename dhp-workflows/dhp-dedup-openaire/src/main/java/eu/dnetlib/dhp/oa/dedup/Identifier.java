package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

import java.io.Serializable;
import java.util.Date;

public class Identifier implements Serializable, Comparable<Identifier>{

    StructuredProperty pid;
    Date date;
    PidType type;

    public Identifier(StructuredProperty pid, Date date, PidType type) {
        this.pid = pid;
        this.date = date;
        this.type = type;
    }

    public StructuredProperty getPid() {
        return pid;
    }

    public void setPid(StructuredProperty pidValue) {
        this.pid = pid;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public PidType getType() {
        return type;
    }

    public void setType(PidType type) {
        this.type = type;
    }

    @Override
    public int compareTo(Identifier i) {
        //priority in comparisons: 1) pidtype, 2) date
        if (this.getType().compareTo(i.getType()) == 0){ //same type
            return this.getDate().compareTo(date);
        }
        else {
            return this.getType().compareTo(i.getType());
        }

    }
}
