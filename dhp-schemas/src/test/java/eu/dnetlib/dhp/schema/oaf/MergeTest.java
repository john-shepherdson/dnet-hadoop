package eu.dnetlib.dhp.schema.oaf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergeTest {

    OafEntity oaf;

    @Before
    public void setUp() {
        oaf = new Publication();
    }

    @Test
    public void mergeListsTest() {

        //string list merge test
        List<String> a = Arrays.asList("a", "b", "c", "e");
        List<String> b = Arrays.asList("a", "b", "c", "d");
        List<String> c = null;

        System.out.println("merge result 1 = " + oaf.mergeLists(a, b));

        System.out.println("merge result 2 = " + oaf.mergeLists(a, c));

        System.out.println("merge result 3 = " + oaf.mergeLists(c, c));
    }

    @Test
    public void mergePublicationCollectedFromTest() {

        Publication a = new Publication();
        Publication b = new Publication();

        a.setCollectedfrom(Arrays.asList(setKV("a", "open"), setKV("b", "closed")));
        b.setCollectedfrom(Arrays.asList(setKV("A", "open"), setKV("b", "Open")));

        a.mergeFrom(b);

        Assert.assertNotNull(a.getCollectedfrom());
        Assert.assertEquals(3, a.getCollectedfrom().size());

    }

    @Test
    public void mergePublicationSubjectTest() {

        Publication a = new Publication();
        Publication b = new Publication();

        a.setSubject(Arrays.asList(setSP("a", "open", "classe"), setSP("b", "open", "classe")));
        b.setSubject(Arrays.asList(setSP("A", "open", "classe"), setSP("c", "open", "classe")));

        a.mergeFrom(b);

        Assert.assertNotNull(a.getSubject());
        Assert.assertEquals(3, a.getSubject().size());

    }

    private KeyValue setKV(final String key, final String value) {

        KeyValue k = new KeyValue();

        k.setKey(key);
        k.setValue(value);

        return k;
    }

    private StructuredProperty setSP(final String value, final String schema, final String classname) {
        StructuredProperty s = new StructuredProperty();
        s.setValue(value);
        Qualifier q = new Qualifier();
        q.setClassname(classname);
        q.setClassid(classname);
        q.setSchemename(schema);
        q.setSchemeid(schema);
        s.setQualifier(q);
        return s;
    }
}
