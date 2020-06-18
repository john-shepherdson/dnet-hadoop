package eu.dnetlib.dhp.oa.graph.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.Creator;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.Metadata;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class SendToZenodo implements Serializable {

    private static final Log log = LogFactory.getLog(SendToZenodo.class);


    public static void main(final String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SendToZenodo.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/graph/dump/upload_zenodo.json")));

        parser.parseArgument(args);


        final String hdfsPath = parser.get("hdfsPath");
        final String hdfsNameNode = parser.get("hdfsNameNode");
        final String access_token = parser.get("accessToken");
        final String connection_url = parser.get("url");
        final String metadata = parser.get("metadata");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsNameNode);

        FileSystem fileSystem = FileSystem.get(conf);


        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
               new Path(hdfsPath), true);
        APIClient apiClient = new APIClient(connection_url, access_token);
        apiClient.connect();
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();

            Path p = fileStatus.getPath();
            String p_string = p.toString();
            String tmp = p_string.substring(0, p_string.lastIndexOf("/") );
            String community = tmp.substring(tmp.lastIndexOf("/") + 1);
            log.info("Sending information for community: " + community);
            fileSystem.copyToLocalFile(p, new Path("/tmp/" + community));


            File f = new File("/tmp/" + community);
            apiClient.upload(f, community);
            apiClient.sendMretadata(metadata);
            apiClient.publish();

            if (f.exists()){
                f.delete();
            }
        }





    }


}