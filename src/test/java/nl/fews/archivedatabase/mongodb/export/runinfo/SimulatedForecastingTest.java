package nl.fews.archivedatabase.mongodb.export.runinfo;

import junit.framework.TestCase;
import org.bson.Document;

public class SimulatedForecastingTest extends TestCase {

	public void testGetRunInfo() {
		Document expected = Document.parse("{\"dispatchTime\": \"dispatchTime\", \"taskRunId\": \"taskRunId\", \"mcId\": \"mcId\", \"userId\": \"userId\", \"time0\": \"time0\", \"workflowId\": \"workflowId\", \"configRevisionNumber\": \"configRevisionNumber\"}");

		SimulatedForecasting simulatedForecasting = new SimulatedForecasting();
		Document document = simulatedForecasting.getRunInfo();
		assertEquals(expected.toJson(), document.toJson());
	}
}