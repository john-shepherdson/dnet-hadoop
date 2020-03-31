package eu.dnetlib.dhp.provision;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

public class RetrieveUpdateFromDatacite {

    public static void main(String[] args) throws Exception{
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(RetrieveUpdateFromDatacite.class.getResourceAsStream("/eu/dnetlib/dhp/provision/retrieve_update_parameters.json")));
        parser.parseArgument(args);
        final String hdfsuri = parser.get("namenode");
        Path hdfswritepath = new Path(parser.get("targetPath"));
        final String timestamp = parser.get("timestamp");


        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        FileSystem.get(URI.create(hdfsuri), conf);

        final AtomicInteger counter = new AtomicInteger(0);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(hdfswritepath), SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Text.class))) {

        }
    }

}
