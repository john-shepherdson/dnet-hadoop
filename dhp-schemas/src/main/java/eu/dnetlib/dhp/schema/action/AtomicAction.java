package eu.dnetlib.dhp.schema.action;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import eu.dnetlib.dhp.schema.oaf.Oaf;

import java.io.Serializable;

@JsonDeserialize(using = AtomicActionDeserializer.class)
public class AtomicAction<T extends Oaf> implements Serializable {

    private Class<T> clazz;

    private T payload;

    public AtomicAction() {
    }

    public AtomicAction(Class<T> clazz, T payload) {
        this.clazz = clazz;
        this.payload = payload;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }
}
