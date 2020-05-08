
package eu.dnetlib.dhp.oa.provision.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Resources;

public class TemplateResources {

	private final String record = read("eu/dnetlib/dhp/oa/provision/template/record.st");

	private final String instance = read("eu/dnetlib/dhp/oa/provision/template/instance.st");

	private final String rel = read("eu/dnetlib/dhp/oa/provision/template/rel.st");

	private final String webresource = read("eu/dnetlib/dhp/oa/provision/template/webresource.st");

	private final String child = read("eu/dnetlib/dhp/oa/provision/template/child.st");

	private final String entity = read("eu/dnetlib/dhp/oa/provision/template/entity.st");

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
