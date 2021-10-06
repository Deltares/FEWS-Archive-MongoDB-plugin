package nl.fews.archivedatabase.mongodb.query.operations;

import nl.fews.archivedatabase.mongodb.query.MongoDbArchiveDatabaseSingleExternalImportRequest;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

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
		Document query = new Document();

		List<String> qualifierId = new ArrayList<>();
		for (int i = 0; i < timeSeriesHeader.getQualifierCount(); i++)
			qualifierId.add(timeSeriesHeader.getQualifierId(i));

		query.append("moduleInstanceId", timeSeriesHeader.getModuleInstanceId());
		query.append("locationId", timeSeriesHeader.getLocationId());
		query.append("parameterId", timeSeriesHeader.getParameterId());
		query.append("qualifierId", "");
		query.append("encodedTimeStepId", timeSeriesHeader.getTimeStep().getEncoded());

		return new MongoDbArchiveDatabaseSingleExternalImportRequest(period, collection, query);
	}
}
