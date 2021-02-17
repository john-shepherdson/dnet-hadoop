/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author D.Pierrakos
 */
public class DownloadReportsListFromDatacite {

	private String dataciteBaseURL;
	private String dataciteReportPath;
	private static final Logger logger = LoggerFactory.getLogger(UsageStatsExporter.class);

	public DownloadReportsListFromDatacite(String dataciteBaseURL, String dataciteReportPath)
		throws MalformedURLException, Exception {

		this.dataciteBaseURL = dataciteBaseURL;
		this.dataciteReportPath = dataciteReportPath;
	}

	public void downloadReportsList() throws ParseException {
		StringBuilder responseStrBuilder = new StringBuilder();

		Gson gson = new Gson();

		try {
			BufferedInputStream in = new BufferedInputStream(new URL(dataciteBaseURL).openStream());
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String inputStr;

			while ((inputStr = streamReader.readLine()) != null) {
				responseStrBuilder.append(inputStr);
			}
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
		JsonObject jsonObject = gson.fromJson(responseStrBuilder.toString(), JsonObject.class);
		JsonArray dataArray = jsonObject.getAsJsonArray("reports");
		ArrayList reportsList = new ArrayList();
		for (JsonElement element : dataArray) {
			reportsList.add(element.getAsJsonObject().get("id").getAsString());
		}

		Iterator it = reportsList.iterator();
		while (it.hasNext()) {
			String reportId = it.next().toString();
			String url = dataciteBaseURL + reportId;

			try {
				BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
				BufferedReader streamReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
				String inputStr;
				StringBuilder responseStrBuilder2 = new StringBuilder();
				while ((inputStr = streamReader.readLine()) != null) {
					responseStrBuilder2.append(inputStr);
				}
				FileSystem fs = FileSystem.get(new Configuration());
				FSDataOutputStream fin = fs
					.create(
						new Path(dataciteReportPath + "/" + reportId + ".json"),
						true);
				byte[] jsonObjectRawBytes = responseStrBuilder2.toString().getBytes();
				fin.write(jsonObjectRawBytes);
				fin.writeChar('\n');

				fin.close();

				fin.close();
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}
}
