
package eu.dnetlib.dhp.application;

import java.io.*;
import java.util.Properties;

public class ApplicationUtils {

	public static void populateOOZIEEnv(final String paramName, String value) throws IOException {
		File file = new File(System.getProperty("oozie.action.output.properties"));
		Properties props = new Properties();

		props.setProperty(paramName, value);
		OutputStream os = new FileOutputStream(file);
		props.store(os, "");
		os.close();
	}

}
