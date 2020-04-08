package eu.dnetlib.dhp.provision.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RetrieveUpdateFromDatacite {

    public static void main(String[] args) throws Exception{
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(RetrieveUpdateFromDatacite.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_retrieve_update_parameters.json")));
        parser.parseArgument(args);
        final String hdfsuri = parser.get("namenode");
        Path hdfswritepath = new Path(parser.get("targetPath"));
        final long timestamp = Long.parseLong(parser.get("timestamp"));
        final String host = parser.get("indexHost");
        final String index = parser.get("indexName");

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        FileSystem.get(URI.create(hdfsuri), conf);
        final Datacite2Scholix d2s = new Datacite2Scholix(RelationMapper.load());
        final ObjectMapper mapper = new ObjectMapper();
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(hdfswritepath), SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Text.class))) {
            final Text value = new Text();
            final IntWritable key = new IntWritable();
            int i = 0;
            for(String dataset: new DataciteClient(host).getDatasetsFromTs(timestamp)) {
                i++;
                List<Scholix> scholix = d2s.generateScholixFromJson(dataset);
                if (scholix!= null)
                    for(Scholix s: scholix) {
                        key.set(i);
                        value.set(mapper.writeValueAsString(s));
                        writer.append(key, value);
                        if (i % 10000 == 0) {
                            System.out.println("wrote "+i);
                        }
                    }
            }

        }
    }
}
