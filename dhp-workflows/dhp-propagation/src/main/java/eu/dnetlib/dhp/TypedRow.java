package eu.dnetlib.dhp;


import eu.dnetlib.dhp.schema.oaf.Author;

import java.io.Serializable;
import java.util.*;

public class TypedRow implements Serializable {
    private String sourceId;
    private String targetId;
    private String type;
    private String value;
    private Set<String> accumulator;
    private List<Author> authors;

    public List<Author> getAuthors() {
        return authors;
    }

    public TypedRow  setAuthors(List<Author> authors) {
        this.authors = authors;
        return this;
    }

    public void addAuthor(Author a){
        if(authors == null){
            authors = new ArrayList<>();
        }
        authors.add(a);
    }

    public Set<String> getAccumulator() {
        return accumulator;
    }

    public TypedRow setAccumulator(Set<String> accumulator) {
        this.accumulator = accumulator;
        return this;
    }


    public void addAll(Set<String> toadd){
        this.accumulator.addAll(toadd);
    }


    public void add(String a){
        if (accumulator == null){
            accumulator = new HashSet<>();
        }
        accumulator.add(a);
    }

    public Iterator<String> getAccumulatorIterator(){
        return accumulator.iterator();
    }

    public String getValue() {
        return value;
    }

    public TypedRow setValue(String value) {
        this.value = value;
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
