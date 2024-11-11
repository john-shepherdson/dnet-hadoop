package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import eu.dnetlib.dhp.schema.oaf.Author;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PublisherAuthors implements Serializable {
    List<Author> publisherAuthorList;

    public PublisherAuthors() {
        this.publisherAuthorList = new ArrayList<>();
    }

    public List<Author> getPublisherAuthorList() {
        return publisherAuthorList;
    }

    public void setPublisherAuthorList(List<Author> publisherAuthorList) {
        this.publisherAuthorList = publisherAuthorList;
    }
}
