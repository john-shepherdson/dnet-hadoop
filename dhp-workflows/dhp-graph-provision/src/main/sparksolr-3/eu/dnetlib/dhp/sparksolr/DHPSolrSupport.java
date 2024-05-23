package eu.dnetlib.dhp.sparksolr;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.rdd.RDD;

public class DHPSolrSupport {

    static public void indexDocs(String zkhost, String collection, int batchSize, RDD<SolrInputDocument> docs) {
        SolrSupport.indexDocs(zkhost, collection, batchSize, docs);
    }
}
