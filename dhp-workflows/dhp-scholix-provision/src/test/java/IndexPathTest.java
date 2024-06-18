import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import eu.dnetlib.sx.index.feeder.IndexFeed;

public class IndexPathTest {

	@Test
	public void testIndexing() throws Exception {
		Configuration conf = new Configuration();
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(conf);
		new IndexFeed(fs).run("file:///Users/sandro/Downloads/scholix/summary_js", "localhost", "summary");

	}

}
