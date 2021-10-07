package nl.fews.archivedatabase.mongodb.query.operations;

import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.text.SimpleDateFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RequestExternalDataImportTest {

	@Test
	void getExternalDataImportRequests() throws Exception {
		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
		timeSeriesHeader.setModuleInstanceId("moduleInstanceId");
		timeSeriesHeader.setLocationId("locationId");
		timeSeriesHeader.setParameterId("parameterId");
		timeSeriesHeader.setQualifierIds("qualifierId");
		timeSeriesHeader.setTimeStep(IrregularTimeStep.INSTANCE);
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
		timeSeriesArray.put(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), 1);
		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = RequestExternalDataImport.getExternalDataImportRequests("", new Period(new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01").getTime()), timeSeriesArrays);

		assertEquals(1, singleExternalDataImportRequests.size());
	}
}