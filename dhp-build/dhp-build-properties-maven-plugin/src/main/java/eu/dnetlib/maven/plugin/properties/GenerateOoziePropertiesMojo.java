
package eu.dnetlib.maven.plugin.properties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;

/**
 * Generates oozie properties which were not provided from commandline.
 *
 * @author mhorst
 * @goal generate-properties
 */
public class GenerateOoziePropertiesMojo extends AbstractMojo {

	public static final String PROPERTY_NAME_WF_SOURCE_DIR = "workflow.source.dir";
	public static final String PROPERTY_NAME_SANDBOX_NAME = "sandboxName";

	private final String[] limiters = {
		"dhp", "dnetlib", "eu"
	};

	@Override
	public void execute() {
		if (System.getProperties().containsKey(PROPERTY_NAME_WF_SOURCE_DIR)
			&& !System.getProperties().containsKey(PROPERTY_NAME_SANDBOX_NAME)) {
			String generatedSandboxName = generateSandboxName(
				System.getProperties().getProperty(PROPERTY_NAME_WF_SOURCE_DIR));
			if (generatedSandboxName != null) {
				System.getProperties().setProperty(PROPERTY_NAME_SANDBOX_NAME, generatedSandboxName);
			} else {
				System.out
					.println(
						"unable to generate sandbox name from path: "
							+ System.getProperties().getProperty(PROPERTY_NAME_WF_SOURCE_DIR));
			}
		}
	}

	/**
	 * Generates sandbox name from workflow source directory.
	 *
	 * @param wfSourceDir workflow source directory
	 * @return generated sandbox name
	 */
	private String generateSandboxName(String wfSourceDir) {
		// utilize all dir names until finding one of the limiters
		List<String> sandboxNameParts = new ArrayList<>();
		String[] tokens = StringUtils.split(wfSourceDir, File.separatorChar);
		ArrayUtils.reverse(tokens);
		if (tokens.length > 0) {
			for (String token : tokens) {
				for (String limiter : limiters) {
					if (limiter.equals(token)) {
						return !sandboxNameParts.isEmpty()
							? StringUtils.join(sandboxNameParts.toArray())
							: null;
					}
				}
				if (!sandboxNameParts.isEmpty()) {
					sandboxNameParts.add(0, File.separator);
				}
				sandboxNameParts.add(0, token);
			}
			return StringUtils.join(sandboxNameParts.toArray());
		} else {
			return null;
		}
	}
}
