
package eu.dnetlib.maven.plugin.properties;

import static eu.dnetlib.maven.plugin.properties.GenerateOoziePropertiesMojo.PROPERTY_NAME_SANDBOX_NAME;
import static eu.dnetlib.maven.plugin.properties.GenerateOoziePropertiesMojo.PROPERTY_NAME_WF_SOURCE_DIR;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author mhorst, claudio.atzori */
class GenerateOoziePropertiesMojoTest {

	private final GenerateOoziePropertiesMojo mojo = new GenerateOoziePropertiesMojo();

	@BeforeEach
	void clearSystemProperties() {
		System.clearProperty(PROPERTY_NAME_SANDBOX_NAME);
		System.clearProperty(PROPERTY_NAME_WF_SOURCE_DIR);
	}

	@Test
	void testExecuteEmpty() throws Exception {
		// execute
		mojo.execute();

		// assert
		assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}

	@Test
	void testExecuteSandboxNameAlreadySet() throws Exception {
		// given
		String workflowSourceDir = "eu/dnetlib/dhp/wf/transformers";
		String sandboxName = "originalSandboxName";
		System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
		System.setProperty(PROPERTY_NAME_SANDBOX_NAME, sandboxName);

		// execute
		mojo.execute();

		// assert
		assertEquals(sandboxName, System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}

	@Test
	void testExecuteEmptyWorkflowSourceDir() throws Exception {
		// given
		String workflowSourceDir = "";
		System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);

		// execute
		mojo.execute();

		// assert
		assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}

	@Test
	void testExecuteNullSandboxNameGenerated() throws Exception {
		// given
		String workflowSourceDir = "eu/dnetlib/dhp/";
		System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);

		// execute
		mojo.execute();

		// assert
		assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}

	@Test
	void testExecute() throws Exception {
		// given
		String workflowSourceDir = "eu/dnetlib/dhp/wf/transformers";
		System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);

		// execute
		mojo.execute();

		// assert
		assertEquals("wf/transformers", System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}

	@Test
	void testExecuteWithoutRoot() throws Exception {
		// given
		String workflowSourceDir = "wf/transformers";
		System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);

		// execute
		mojo.execute();

		// assert
		assertEquals("wf/transformers", System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
	}
}
