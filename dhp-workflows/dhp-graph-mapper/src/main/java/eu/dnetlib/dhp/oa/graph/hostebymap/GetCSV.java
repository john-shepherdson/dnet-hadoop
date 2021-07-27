package eu.dnetlib.dhp.oa.graph.hostebymap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import eu.dnetlib.dhp.oa.graph.hostebymap.model.UnibiGoldModel;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class GetCSV {
    private static final Log log = LogFactory.getLog(eu.dnetlib.dhp.oa.graph.hostebymap.GetCSV.class);

    public static void main(final String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                GetCSV.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/graph/hostedbymap/download_csv_parameters.json")));

        parser.parseArgument(args);

        final String fileURL = parser.get("fileURL");
        final String hdfsPath = parser.get("hdfsPath");
        final String hdfsNameNode = parser.get("hdfsNameNode");
        final String classForName = parser.get("classForName");
        final Boolean shouldReplace = Optional.ofNullable((parser.get("replace")))
                .map(Boolean::valueOf)
                .orElse(false);


        URLConnection connection = new URL(fileURL).openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
        connection.connect();

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));

        if(shouldReplace){
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/DOAJ.csv")));
            String line = null;
            while((line = in.readLine())!= null){
                writer.println(line.replace("\\\"", "\""));
            }
            writer.close();
            in.close();
            in = new BufferedReader(new FileReader("/tmp/DOAJ.csv"));
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsNameNode);

        FileSystem fileSystem = FileSystem.get(conf);
        Path hdfsWritePath = new Path(hdfsPath);
        FSDataOutputStream fsDataOutputStream = null;
        if (fileSystem.exists(hdfsWritePath)) {
            fileSystem.delete(hdfsWritePath, false);
        }
        fsDataOutputStream = fileSystem.create(hdfsWritePath);

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

        Class<?> clazz = Class.forName(classForName);

        ObjectMapper mapper = new ObjectMapper();

        new CsvToBeanBuilder(in)
                .withType(clazz)
                .withMultilineLimit(1)
                .build()
                .parse()
        .forEach(line -> {
            try {
                writer.write(mapper.writeValueAsString(line));
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });



        writer.close();
        in.close();
        if(shouldReplace){
            File f = new File("/tmp/DOAJ.csv");
            f.delete();
        }


    }




}

