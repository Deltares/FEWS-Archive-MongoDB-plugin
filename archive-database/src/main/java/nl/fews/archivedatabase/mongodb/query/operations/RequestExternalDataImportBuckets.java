package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.MongoDbArchiveDatabaseSingleExternalImportRequest;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class RequestExternalDataImportBuckets extends RequestExternalDataImportBase {
	/**
	 *
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getExternalDataImportRequests(String collection, Period period, TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
		try{
			List<SingleExternalDataImportRequest> singleExternalDataImportRequests = new ArrayList<>();
			ForkJoinPool pool = new ForkJoinPool(Settings.get("databaseBaseThreads"));
			ArrayList<Callable<SingleExternalDataImportRequest>> tasks = new ArrayList<>();
			for (int i = 0; i < timeSeriesArrays.size(); i++) {
				TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = timeSeriesArrays.get(i);
				if(!timeSeriesArray.isCompletelyReliable() || timeSeriesArray.getHeader().getTimeStep() == IrregularTimeStep.INSTANCE)
					tasks.add(() -> getExternalDataImportRequest(collection, period, timeSeriesArray));
			}
			List<Future<SingleExternalDataImportRequest>> results = pool.invokeAll(tasks);
			for (Future<SingleExternalDataImportRequest> x : results) {
				SingleExternalDataImportRequest singleExternalDataImportRequest = x.get();
				if(singleExternalDataImportRequest != null)
					singleExternalDataImportRequests.add(singleExternalDataImportRequest);
			}
			pool.shutdown();
			return singleExternalDataImportRequests;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param collection collection
	 * @param period period
	 * @param timeSeriesArray timeSeriesArray
	 * @return SingleExternalDataImportRequest
	 */
	private SingleExternalDataImportRequest getExternalDataImportRequest(String collection, Period period, TimeSeriesArray<TimeSeriesHeader> timeSeriesArray){
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
