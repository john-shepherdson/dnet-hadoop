
package eu.dnetlib.dhp.oozie;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

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
