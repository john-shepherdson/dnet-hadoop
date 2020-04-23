package eu.dnetlib.dhp;

import com.google.gson.Gson;
import eu.dnetlib.dhp.community.CommunityConfiguration;
import eu.dnetlib.dhp.community.CommunityConfigurationFactory;
import eu.dnetlib.dhp.community.Constraint;
import eu.dnetlib.dhp.community.SelectionConstraints;
import eu.dnetlib.dhp.selectioncriteria.VerbResolver;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Created by miriam on 03/08/2018. */
public class CommunityConfigurationFactoryTest {

    private final VerbResolver resolver = new VerbResolver();

    @Test
    public void parseTest() throws DocumentException, IOException {
        String xml =
                IOUtils.toString(
                        getClass()
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/communityconfiguration/community_configuration.xml"));
        final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
        Assertions.assertEquals(5, cc.size());
        cc.getCommunityList()
                .forEach(c -> Assertions.assertTrue(StringUtils.isNoneBlank(c.getId())));
    }

    @Test
    public void applyVerb()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
                    InstantiationException {
        Constraint sc = new Constraint();
        sc.setVerb("not_contains");
        sc.setField("contributor");
        sc.setValue("DARIAH");
        sc.setSelection(resolver.getSelectionCriteria(sc.getVerb(), sc.getValue()));
        String metadata = "This work has been partially supported by DARIAH-EU infrastructure";
        Assertions.assertFalse(sc.verifyCriteria(metadata));
    }

    @Test
    public void loadSelCriteriaTest() throws DocumentException, IOException {
        String xml =
                IOUtils.toString(
                        getClass()
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/communityconfiguration/community_configuration_selcrit.xml"));
        final CommunityConfiguration cc = CommunityConfigurationFactory.newInstance(xml);
        Map<String, List<String>> param = new HashMap<>();
        param.put("author", new ArrayList<>(Collections.singletonList("Pippo Pippi")));
        param.put(
                "description",
                new ArrayList<>(
                        Collections.singletonList(
                                "This work has been partially supported by DARIAH-EU infrastructure")));
        param.put(
                "contributor",
                new ArrayList<>(
                        Collections.singletonList(
                                "Pallino ha aiutato a scrivere il paper. Pallino lavora per DARIAH")));
        List<String> comm =
                cc.getCommunityForDatasource(
                        "openaire____::1cfdb2e14977f31a98e0118283401f32", param);
        Assertions.assertEquals(1, comm.size());
        Assertions.assertEquals("dariah", comm.get(0));
    }

    @Test
    public void test4() throws DocumentException, IOException {
        final CommunityConfiguration cc =
                CommunityConfigurationFactory.fromJson(
                        IOUtils.toString(
                                getClass()
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/communityconfiguration/community_configuration_selcrit.json")));
        cc.toString();
    }

    @Test
    public void test5() throws IOException, DocumentException {

        // final CommunityConfiguration cc =
        // CommunityConfigurationFactory.newInstance(IOUtils.toString(getClass().getResourceAsStream("test.xml")));
        final CommunityConfiguration cc =
                CommunityConfigurationFactory.fromJson(
                        IOUtils.toString(
                                getClass()
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/communityconfiguration/community_configuration.json")));

        System.out.println(cc.toJson());
    }

    @Test
    public void test6() {
        String json =
                "{\"criteria\":[{\"constraint\":[{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}]}]}";

        String step1 = "{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}";

        Constraint c = new Gson().fromJson(step1, Constraint.class);
        //
        //        String step2 =
        // "{\"constraint\":[{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}]}";
        //
        //        ConstraintEncapsulator ce = new
        // Gson().fromJson(step2,ConstraintEncapsulator.class);
        //
        //
        //        String step3 =
        // "{\"ce\":{\"constraint\":[{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}]}}";
        //
        //        Constraints cons = new Gson().fromJson(step3,Constraints.class);
        //
        //        String step4 =
        // "{\"criteria\":[{\"ce\":{\"constraint\":[{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}]}}]}";
        //
        //        ConstraintsList cl = new Gson().fromJson(step4,ConstraintsList.class);
        //
        //        String step5 =
        // "{\"cl\":{\"criteria\":[{\"ce\":{\"constraint\":[{\"verb\":\"contains\",\"field\":\"contributor\",\"value\":\"DARIAH\"}]}}]}}";
        SelectionConstraints sl = new Gson().fromJson(json, SelectionConstraints.class);
    }

    @Test
    public void test7() throws IOException {
        final CommunityConfiguration cc =
                CommunityConfigurationFactory.fromJson(
                        IOUtils.toString(
                                getClass()
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/communityconfiguration/tagging_conf.json")));

        System.out.println(cc.toJson());
    }
}
