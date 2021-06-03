package nl.fews.archivedatabase.mongodb.export.runinfo;

import junit.framework.TestCase;
import org.bson.Document;

public class ExternalHistoricalTest extends TestCase {

	public void testGetRunInfo() {
		Document expected = Document.parse("{}");

		ExternalHistorical externalHistorical = new ExternalHistorical();
		Document document = externalHistorical.getRunInfo();
		assertEquals(expected.toJson(), document.toJson());
	}
}