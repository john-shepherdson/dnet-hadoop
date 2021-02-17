/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.indicators.export;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * @author D. Pierrakos
 */
public class ExecuteWorkflow {

	private static final Logger logger = LoggerFactory.getLogger(ExecuteWorkflow.class);

	public static void main(String args[]) throws Exception {

		// Sending the logs to the console
		BasicConfigurator.configure();

		logger.info("Workflow Executed");
	}

}
