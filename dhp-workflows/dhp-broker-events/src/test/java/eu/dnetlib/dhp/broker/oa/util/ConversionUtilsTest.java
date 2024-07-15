
package eu.dnetlib.dhp.broker.oa.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ConversionUtilsTest {

	@BeforeEach
	public void setUp() throws Exception {}

	@Test
	public void testAllResultPids() {
		final Qualifier qf = new Qualifier();
		qf.setClassid("test");
		qf.setClassname("test");
		qf.setSchemeid("test");
		qf.setSchemename("test");

		final StructuredProperty sp1 = new StructuredProperty();
		sp1.setValue("1");
		sp1.setQualifier(qf);

		final StructuredProperty sp2 = new StructuredProperty();
		sp2.setValue("2");
		sp2.setQualifier(qf);

		final StructuredProperty sp3 = new StructuredProperty();
		sp3.setValue("3");
		sp3.setQualifier(qf);

		final StructuredProperty sp4a = new StructuredProperty();
		sp4a.setValue("4");
		sp4a.setQualifier(qf);

		final StructuredProperty sp4b = new StructuredProperty();
		sp4b.setValue("4");
		sp4b.setQualifier(qf);

		final StructuredProperty sp5 = new StructuredProperty();
		sp5.setValue("5");
		sp5.setQualifier(qf);

		final StructuredProperty sp6a = new StructuredProperty();
		sp6a.setValue("6");
		sp6a.setQualifier(qf);

		final StructuredProperty sp6b = new StructuredProperty();
		sp6b.setValue("6");
		sp6b.setQualifier(qf);

		final Result oaf = new Result();
		oaf.setPid(new ArrayList<>());
		oaf.getPid().add(sp1);
		oaf.getPid().add(sp2);
		oaf.getPid().add(sp4a);

		final Instance instance1 = new Instance();
		instance1.setPid(new ArrayList<>());
		instance1.setAlternateIdentifier(new ArrayList<>());
		instance1.getPid().add(sp3);
		instance1.getPid().add(sp4b);
		instance1.getAlternateIdentifier().add(sp5);
		instance1.getAlternateIdentifier().add(sp6a);

		final Instance instance2 = new Instance();
		instance2.setPid(new ArrayList<>());
		instance2.setAlternateIdentifier(new ArrayList<>());
		instance2.getPid().add(sp6b);

		oaf.setInstance(new ArrayList<>());
		oaf.getInstance().add(instance1);
		oaf.getInstance().add(instance2);

		final List<OaBrokerTypedValue> list = ConversionUtils.allResultPids(oaf);

		// list.forEach(x -> System.out.println(x.getValue()));

		assertEquals(6, list.size());
	}

	public void testOafResultToBrokerResult() {

		final Author a1 = createAuthor("Michele Artini", "0000-0002-4406-428X");
		final Author a2 = createAuthor("Claudio Atzori", "http://orcid.org/0000-0001-9613-6639");
		final Author a3 = createAuthor("Alessia Bardi", null);

		final Result r = new Result();
		r.setAuthor(Arrays.asList(a1, a2, a3));

		final OaBrokerMainEntity br = ConversionUtils.oafResultToBrokerResult(r);

		assertEquals(3, br.getCreators().size());
		assertEquals("0000-0002-4406-428X", br.getCreators().get(0).getOrcid());
		assertEquals("0000-0001-9613-6639", br.getCreators().get(1).getOrcid());
		assertNull(br.getCreators().get(2).getOrcid());
	}

	private Author createAuthor(final String name, final String orcid) {

		final Author a = new Author();
		a.setFullname("Michele Artini");

		if (orcid != null) {
			final Qualifier q = new Qualifier();
			q.setClassid(ModelConstants.ORCID);
			q.setClassname(ModelConstants.ORCID);
			q.setSchemeid("dnet:pids");
			q.setSchemename("dnet:pids");

			final StructuredProperty pid = new StructuredProperty();
			pid.setQualifier(q);
			pid.setValue(orcid);

			a.setPid(Arrays.asList(pid));
		}
		return a;
	}

}
