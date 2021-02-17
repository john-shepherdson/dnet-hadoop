/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class ExecuteWorkflow {

	static String dataciteBaseURL;
	static String dataciteReportPath;
	static String dbHiveUrl;
	static String dbImpalaUrl;
	static String datasetUsageStatsDBSchema;
	static String statsDBSchema;
	static boolean recreateDbAndTables;
	static boolean datasetsEmptyDirs;
	static boolean finalTablesVisibleToImpala;

	public static void main(String args[]) throws Exception {

		// Sending the logs to the console
		BasicConfigurator.configure();

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					UsageStatsExporter.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/datasetsusagestats/export/datasets_usagestats_parameters.json")));
		parser.parseArgument(args);

		// Setting up the initial parameters
		dataciteBaseURL = parser.get("dataciteBaseURL");
		dataciteReportPath = parser.get("dataciteReportPath");
		dbHiveUrl = parser.get("dbHiveUrl");
		dbImpalaUrl = parser.get("dbImpalaUrl");
		datasetUsageStatsDBSchema = parser.get("datasetUsageStatsDBSchema");
		statsDBSchema = parser.get("statsDBSchema");

		if (parser.get("recreateDbAndTables").toLowerCase().equals("true"))
			recreateDbAndTables = true;
		else
			recreateDbAndTables = false;

		if (parser.get("datasetsEmptyDirs").toLowerCase().equals("true"))
			datasetsEmptyDirs = true;
		else
			datasetsEmptyDirs = false;

//		if (parser.get("finalTablesVisibleToImpala").toLowerCase().equals("true"))
//			finalTablesVisibleToImpala = true;
//		else
//			finalTablesVisibleToImpala = false;
//
		UsageStatsExporter usagestatsExport = new UsageStatsExporter();
		usagestatsExport.export();
	}

}
