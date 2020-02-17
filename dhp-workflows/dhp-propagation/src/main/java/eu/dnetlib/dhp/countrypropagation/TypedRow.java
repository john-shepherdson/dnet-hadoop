package eu.dnetlib.dhp.countrypropagation;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TypedRow implements Serializable {
    private String sourceId;
    private String targetId;
    private String type;
    private String country;

    public List<String> getAccumulator() {
        return accumulator;
    }

    public TypedRow setAccumulator(List<String> accumulator) {
        this.accumulator = accumulator;
        return this;
    }

    private List<String> accumulator;


    public void add(String a){
        if (accumulator == null){
            accumulator = new ArrayList<>();
        }
        accumulator.add(a);
    }

    public Iterator<String> getAccumulatorIterator(){
        return accumulator.iterator();
    }

    public String getCountry() {
        return country;
    }

    public TypedRow setCountry(String country) {
        this.country = country;
        return this;
    }

    public String getSourceId() {
        return sourceId;
    }
    public TypedRow setSourceId(String sourceId) {
        this.sourceId = sourceId;
        return this;
    }
    public String getTargetId() {
        return targetId;
    }
    public TypedRow setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public String getType() {
        return type;
    }
    public TypedRow setType(String type) {
        this.type = type;
        return this;
    }

}
