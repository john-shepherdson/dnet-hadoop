package eu.dnetlib.dhp.graph.utils;

import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TemplateResources {

    private String record = read("eu/dnetlib/dhp/graph/template/record.st");

    private String instance = read("eu/dnetlib/dhp/graph/template/instance.st");

    private String rel = read("eu/dnetlib/dhp/graph/template/rel.st");

    private String webresource = read("eu/dnetlib/dhp/graph/template/webresource.st");

    private String child = read("eu/dnetlib/dhp/graph/template/child.st");

    private String entity = read("eu/dnetlib/dhp/graph/template/entity.st");

    private static String read(final String classpathResource) throws IOException {
        return Resources.toString(Resources.getResource(classpathResource), StandardCharsets.UTF_8);
    }

    public TemplateResources() throws IOException {

    }

    public String getEntity() {
        return entity;
    }

    public String getRecord() {
        return record;
    }

    public String getInstance() {
        return instance;
    }

    public String getRel() {
        return rel;
    }

    public String getWebresource() {
        return webresource;
    }

    public String getChild() {
        return child;
    }

}
