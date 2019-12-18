package eu.dnetlib.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SparkCreateDedupTest {

    String configuration;
    String entity = "publication";

    @Before
    public void setUp() throws IOException {
        configuration = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dedup/conf/pub_dt.curr.conf.json"));

    }

    @Test
    @Ignore
    public void createSimRelsTest() throws Exception {
        SparkCreateSimRels.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void createCCTest() throws Exception {

        SparkCreateConnectedComponent.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-t", "/tmp/dedup",
        });
    }

    @Test
    @Ignore
    public void dedupRecordTest() throws Exception {
        SparkCreateDedupRecord.main(new String[] {
                "-mt", "local[*]",
                "-s", "/home/sandro/betadump",
                "-e", entity,
                "-c", ArgumentApplicationParser.compressArgument(configuration),
                "-d", "/tmp/dedup",
        });
    }

    @Test
    public void printCC() throws Exception {
        System.out.println(ArgumentApplicationParser.compressArgument(configuration));
    }


//            [20|grid________::6031f94bef015a37783268ec1e75f17f, 20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46]
//            [20|grid________::672e1e5cef49e68f124d3da5225a7357, 20|grid________::7a402604c3853c7a0af14f88f56bf7e1]
//            [20|grid________::2fc05b35e11d915b220a66356053eae2, 20|grid________::b02fb3176eb38f6c572722550c07e7ab]
//            [20|grid________::bc86248ab2b8d7955dcaf592ba342262, 20|corda_______::45a8ec964029278fb938805182e247a8]
//            [20|doajarticles::74551f800ad1c81a6cd31c5162887b7f, 20|rcuk________::86dc9a83df05a58917f38ca09f814617]
//            [20|nsf_________::5e837d8e6444cc298db314ea54ad2f4a, 20|snsf________::7b54715f0ec5c6a0a44672f45d98be8d]
//            [20|corda__h2020::7ee7e57bad06b92c1a568dd61e10ba8c, 20|snsf________::2d4a2695221a3ce0c749ee34e064c0b3]
//            [20|corda_______::25220a523550176dac9e5432dac43596, 20|grid________::9782f16a46650cbbfaaa2315109507d1]
//            [20|nih_________::88c3b664dcc7af9e827f94ac964cd66c, 20|grid________::238d3ac0a7d119d5c8342a647f5245f5]
//            [20|rcuk________::0582c20fcfb270f9ec1b19b0f0dcd881, 20|nsf_________::9afa48ddf0bc2cd4f3c41dc41daabcdb]
//            [20|rcuk________::fbc445f8d24e569bc8b640dba86ae978, 20|corda_______::5a8a4094f1b68a88fc56e65cea7ebfa0]
//            [20|rcuk________::7485257cd5caaf6316ba8062feea801d, 20|grid________::dded811e5f5a4c9f7ca8f9955e52ade7]
//            [20|nih_________::0576dd270d29d5b7c23dd15a827ccdb9, 20|corda_______::10ca69f6a4a121f75fdde1feee226ce0]
//            [20|corda__h2020::0429f6addf10e9b2939d65c6fb097ffd, 20|grid________::6563ec73057624d5ccc0cd050b302181]

    @Test
    public void testHashCode() {
        final String s1 = "20|grid________::6031f94bef015a37783268ec1e75f17f";
        final String s2 = "20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46";

        final HashFunction hashFunction = Hashing.murmur3_128();

        System.out.println( s1.hashCode());
        System.out.println(hashFunction.hashUnencodedChars(s1).asLong());
        System.out.println( s2.hashCode());
        System.out.println(hashFunction.hashUnencodedChars(s2).asLong());

    }


}
