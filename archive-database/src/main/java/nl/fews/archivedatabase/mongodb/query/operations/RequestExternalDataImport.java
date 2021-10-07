package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.MongoDbArchiveDatabaseSingleExternalImportRequest;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.IrregularTimeStep;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public final class RequestExternalDataImport {

	private RequestExternalDataImport() {
	}

	/**
	 *
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	public static List<SingleExternalDataImportRequest> getExternalDataImportRequests(String collection, Period period, TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = new ArrayList<>();
		for (int i = 0; i < timeSeriesArrays.size(); i++) {
			TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = timeSeriesArrays.get(i);
			if(!timeSeriesArray.isCompletelyReliable() || timeSeriesArray.getHeader().getTimeStep() == IrregularTimeStep.INSTANCE) {
				singleExternalDataImportRequests.add(RequestExternalDataImport.getExternalDataImportRequest(collection, period, timeSeriesArray));
			}
		}
		return singleExternalDataImportRequests;
	}

	/**
	 *
	 * @param collection collection
	 * @param period period
	 * @param timeSeriesArray timeSeriesArray
	 * @return SingleExternalDataImportRequest
	 */
	private static SingleExternalDataImportRequest getExternalDataImportRequest(String collection, Period period, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray){
		TimeSeriesHeader timeSeriesHeader = timeSeriesArray.getHeader();
		Map<String, List<Object>> query = new HashMap<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int i = 0; i < timeSeriesHeader.getQualifierCount(); i++)
			qualifierIds.add(timeSeriesHeader.getQualifierId(i));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

		query.put("moduleInstanceId", List.of(timeSeriesHeader.getModuleInstanceId()));
		query.put("locationId", List.of(timeSeriesHeader.getLocationId()));
		query.put("parameterId", List.of(timeSeriesHeader.getParameterId()));
		query.put("qualifierId", List.of(qualifierId));
		query.put("encodedTimeStepId", List.of(timeSeriesHeader.getTimeStep().getEncoded()));

		return new MongoDbArchiveDatabaseSingleExternalImportRequest(period, collection, query, timeSeriesArray);
	}
}
