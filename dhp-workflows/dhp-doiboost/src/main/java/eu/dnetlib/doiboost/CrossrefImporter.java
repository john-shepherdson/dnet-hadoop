package eu.dnetlib.doiboost;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpHost;



public class CrossrefImporter {



    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(CrossrefImporter.class.getResourceAsStream("/eu/dnetlib/dhp/doiboost/import_from_es.json")));
        parser.parseArgument(args);
        System.out.println(parser.get("targetPath"));


        final String hdfsuri = parser.get("namenode");
        System.out.println(hdfsuri);
        Path hdfswritepath = new Path(parser.get("targetPath"));


        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());



        ESClient client = new ESClient("ip-90-147-167-25.ct1.garrservices.it", "crossref");

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(hdfswritepath), SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Text.class))) {


            int i = 0;
            long start= System.currentTimeMillis();
            long end = 0;
            final IntWritable key = new IntWritable(i);
            final Text value = new Text();
            while (client.hasNext()) {
                key.set(i++);
                value.set(client.next());
                writer.append(key, value);
                if (i % 100000 == 0) {
                    end = System.currentTimeMillis();
                    final float time = (end - start) / 1000;
                    System.out.println(String.format("Imported %d records last 100000 imported in %f seconds", i, time));
                    start = System.currentTimeMillis();
                }
            }
        }
    }
}
