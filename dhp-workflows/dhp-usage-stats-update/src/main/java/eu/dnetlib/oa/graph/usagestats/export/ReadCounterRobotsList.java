/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.usagestats.export;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
/**
 * @author D. Pierrakos, S. Zoupanos
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReadCounterRobotsList {

	private ArrayList robotsPatterns = new ArrayList();
	private String COUNTER_ROBOTS_URL;

	public ReadCounterRobotsList(String url) throws IOException, JSONException, ParseException {
		COUNTER_ROBOTS_URL = url;
		robotsPatterns = readRobotsPartners(COUNTER_ROBOTS_URL);
	}

	private ArrayList readRobotsPartners(String url) throws MalformedURLException, IOException, ParseException {
		InputStream is = new URL(url).openStream();
		JSONParser parser = new JSONParser();
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("ISO-8859-1")));
		JSONArray jsonArray = (JSONArray) parser.parse(reader);
		for (Object aJsonArray : jsonArray) {
			org.json.simple.JSONObject jsonObjectRow = (org.json.simple.JSONObject) aJsonArray;
			robotsPatterns.add(jsonObjectRow.get("pattern").toString().replace("\\", "\\\\"));
		}
		return robotsPatterns;
	}

	public ArrayList getRobotsPatterns() {
		return robotsPatterns;
	}
}
