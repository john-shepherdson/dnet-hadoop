/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.dnetlib.oa.graph.usagestatsbuild.export;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class ExecuteWorkflow {

    static String matomoAuthToken;
    static String matomoBaseURL;
    static String repoLogPath;
    static String portalLogPath;
    static String portalMatomoID;
    static String irusUKBaseURL;
    static String irusUKReportPath;
    static String sarcsReportPathArray;
    static String sarcsReportPathNonArray;
    static String lareferenciaLogPath;
    static String lareferenciaBaseURL;
    static String lareferenciaAuthToken;
    static String dbHiveUrl;
    static String dbImpalaUrl;
    static String usageRawDataDBSchema;
    static String usageStatsDBSchema;
    static String statsDBSchema;
    static boolean recreateDbAndTables;

    static boolean piwikEmptyDirs;
    static boolean downloadPiwikLogs;
    static boolean processPiwikLogs;

    static Calendar startingLogPeriod;
    static Calendar endingLogPeriod;
    static int numberOfPiwikIdsToDownload;
    static int numberOfSiteIdsToDownload;

    static boolean laReferenciaEmptyDirs;
    static boolean downloadLaReferenciaLogs;
    static boolean processLaReferenciaLogs;

    static boolean irusCreateTablesEmptyDirs;
    static boolean irusDownloadReports;
    static boolean irusProcessStats;
    static int irusNumberOfOpendoarsToDownload;

    static boolean sarcCreateTablesEmptyDirs;
    static boolean sarcDownloadReports;
    static boolean sarcProcessStats;
    static int sarcNumberOfIssnToDownload;

    static boolean finalizeStats;
    static boolean finalTablesVisibleToImpala;

    static int numberOfDownloadThreads;

    private static final Logger logger = LoggerFactory.getLogger(PiwikStatsDB.class);

    public static void main(String args[]) throws Exception {

        // Sending the logs to the console
        BasicConfigurator.configure();

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                UsageStatsExporter.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/graph/usagestatsbuild/export/usagestatsbuild_parameters.json")));
        parser.parseArgument(args);

        // Setting up the initial parameters
        matomoAuthToken = parser.get("matomoAuthToken");
        matomoBaseURL = parser.get("matomoBaseURL");
        repoLogPath = parser.get("repoLogPath");
        portalLogPath = parser.get("portalLogPath");
        portalMatomoID = parser.get("portalMatomoID");
        irusUKBaseURL = parser.get("irusUKBaseURL");
        irusUKReportPath = parser.get("irusUKReportPath");
        sarcsReportPathArray = parser.get("sarcsReportPathArray");
        sarcsReportPathNonArray = parser.get("sarcsReportPathNonArray");
        lareferenciaLogPath = parser.get("lareferenciaLogPath");
        lareferenciaBaseURL = parser.get("lareferenciaBaseURL");
        lareferenciaAuthToken = parser.get("lareferenciaAuthToken");

        dbHiveUrl = parser.get("dbHiveUrl");
        dbImpalaUrl = parser.get("dbImpalaUrl");
        usageRawDataDBSchema = parser.get("usageRawDataDBSchema");
        usageStatsDBSchema = parser.get("usageStatsDBSchema");
        statsDBSchema = parser.get("statsDBSchema");

        if (parser.get("processPiwikLogs").toLowerCase().equals("true")) {
            processPiwikLogs = true;
        } else {
            processPiwikLogs = false;
        }

        String startingLogPeriodStr = parser.get("startingLogPeriod");
        Date startingLogPeriodDate = new SimpleDateFormat("MM/yyyy").parse(startingLogPeriodStr);
        startingLogPeriod = startingLogPeriodStr(startingLogPeriodDate);

        String endingLogPeriodStr = parser.get("endingLogPeriod");
        Date endingLogPeriodDate = new SimpleDateFormat("MM/yyyy").parse(endingLogPeriodStr);
        endingLogPeriod = startingLogPeriodStr(endingLogPeriodDate);

        numberOfPiwikIdsToDownload = Integer.parseInt(parser.get("numberOfPiwikIdsToDownload"));
        numberOfSiteIdsToDownload = Integer.parseInt(parser.get("numberOfSiteIdsToDownload"));

        if (parser.get("recreateDbAndTables").toLowerCase().equals("true")) {
            recreateDbAndTables = true;
        } else {
            recreateDbAndTables = false;
        }

        if (parser.get("processLaReferenciaLogs").toLowerCase().equals("true")) {
            processLaReferenciaLogs = true;
        } else {
            processLaReferenciaLogs = false;
        }

        if (parser.get("irusProcessStats").toLowerCase().equals("true")) {
            irusProcessStats = true;
        } else {
            irusProcessStats = false;
        }

        irusNumberOfOpendoarsToDownload = Integer.parseInt(parser.get("irusNumberOfOpendoarsToDownload"));

        if (parser.get("sarcProcessStats").toLowerCase().equals("true")) {
            sarcProcessStats = true;
        } else {
            sarcProcessStats = false;
        }
        sarcNumberOfIssnToDownload = Integer.parseInt(parser.get("sarcNumberOfIssnToDownload"));

        if (parser.get("finalizeStats").toLowerCase().equals("true")) {
            finalizeStats = true;
        } else {
            finalizeStats = false;
        }
        if (parser.get("finalTablesVisibleToImpala").toLowerCase().equals("true")) {
            finalTablesVisibleToImpala = true;
        } else {
            numberOfDownloadThreads = Integer.parseInt(parser.get("numberOfDownloadThreads"));
        }

        UsageStatsExporter usagestatsExport = new UsageStatsExporter();
        usagestatsExport.export();
    }

    private static Calendar startingLogPeriodStr(Date date) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;

    }
}
