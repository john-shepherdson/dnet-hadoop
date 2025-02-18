
package eu.dnetlib.dhp.oozie;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class RunJavaSparkJob {
	private static final Logger log = LoggerFactory.getLogger(RunJavaSparkJob.class);

	private final ArgumentApplicationParser parser;

	public RunJavaSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {

		String className = args[0];
		String[] mainArgs = new String[args.length - 1];
		System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);

		SparkConf conf = new SparkConf();

		runWithSparkHiveSession(
			conf,
			false,
			spark -> {
				Class<?> clazz = Class.forName(className);

				Method mainMethod = clazz.getMethod("main", String[].class);

				if (!java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers())) {
					System.err.println("Error: The main method of " + className + " must be static.");
					System.exit(1);
				}

				mainMethod.invoke(null, (Object) mainArgs);
			});
	}
}
