package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.Filter;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.query.operations.ReadBuckets;
import nl.fews.archivedatabase.mongodb.query.operations.RequestExternalDataImportBuckets;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class MongoDbArchiveDatabaseTimeSeriesReader implements ArchiveDatabaseTimeSeriesReader {

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.mongodb";

	/**
	 *
	 */
	private static MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = null;

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static MongoDbArchiveDatabaseTimeSeriesReader create() {
		if(mongoDbArchiveDatabaseTimeSeriesReader == null)
			mongoDbArchiveDatabaseTimeSeriesReader = new MongoDbArchiveDatabaseTimeSeriesReader();
		return mongoDbArchiveDatabaseTimeSeriesReader;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider
	 */
	@Override
	public void setHeaderProvider(FewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider) {
		Settings.put("headerProvider", fewsTimeSeriesHeaderProvider);
	}

	/**
	 *
	 * @param singleExternalDataImportRequest singleExternalDataImportRequest
	 * @return TimeSeriesArrays<FewsTimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> importObservedData(SingleExternalDataImportRequest singleExternalDataImportRequest) {
		MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
		MongoCursor<Document> results = new ReadBuckets().read(
				mongoDbArchiveDatabaseSingleExternalImportRequest.getCollection(),
				mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery(),
				mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate(),
				mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate());
		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesArray();
		if(results.hasNext()){
			Document result = results.next();
			Map<Long, Double> resultMap = result.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t").getTime(), s -> s.getDouble("v")));
			for (int i = 0; i < timeSeriesArray.size(); i++) {
				long time = timeSeriesArray.getTime(i);
				if(timeSeriesArray.isValueReliable(i) && resultMap.containsKey(time)) {
					timeSeriesArray.setValue(i, resultMap.get(time).floatValue());
				}
			}
		}
		return new TimeSeriesArrays<>(timeSeriesArray);
	}

	/**
	 *
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getObservedDataImportRequest(Period period, TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of Period Must Fall On or After Start of Period");

		return new RequestExternalDataImportBuckets().getExternalDataImportRequests(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), period, timeSeriesArrays);
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseReadResult
	 */
	@Override
	public ArchiveDatabaseReadResult read(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		if(archiveDatabaseResultSearchParameters.getPeriod().getEndDate().before(archiveDatabaseResultSearchParameters.getPeriod().getStartDate()))
			throw new IllegalArgumentException("End of Period Must Fall On or After Start of Period");

		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType());
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			Map<String, List<Object>> query = new HashMap<>();

			query.put("metaData.areaId", List.of(archiveDatabaseResultSearchParameters.getAreaId()));

			if(archiveDatabaseResultSearchParameters.getSourceIds() != null && !archiveDatabaseResultSearchParameters.getSourceIds().isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getSourceIds()));

			if(archiveDatabaseResultSearchParameters.getParameterIds() != null && !archiveDatabaseResultSearchParameters.getParameterIds().isEmpty())
				query.put("parameterId", new ArrayList<>(archiveDatabaseResultSearchParameters.getParameterIds()));

			if(archiveDatabaseResultSearchParameters.getModuleInstanceIds() != null && !archiveDatabaseResultSearchParameters.getModuleInstanceIds().isEmpty())
				query.put("moduleInstanceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getModuleInstanceIds()));

			if(archiveDatabaseResultSearchParameters.getTimeSteps() != null && !archiveDatabaseResultSearchParameters.getTimeSteps().isEmpty())
				query.put("encodedTimeStepId", new ArrayList<>(archiveDatabaseResultSearchParameters.getTimeSteps().stream().map(TimeStep::getEncoded).collect(Collectors.toList())));

			Read read = (Read)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Read%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			MongoCursor<Document> result = read.read(collection, query, archiveDatabaseResultSearchParameters.getPeriod().getStartDate(), archiveDatabaseResultSearchParameters.getPeriod().getEndDate());

			return new MongoDbArchiveDatabaseReadResult(result, TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType());
		}
		catch(Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseSummary
	 */
	@Override
	public ArchiveDatabaseSummary getSummary(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		if(archiveDatabaseResultSearchParameters.getPeriod().getEndDate().before(archiveDatabaseResultSearchParameters.getPeriod().getStartDate()))
			throw new IllegalArgumentException("End of Period Must Fall On or After Start of Period");

		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType());
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			Map<String, List<Object>> query = new HashMap<>();

			query.put("metaData.areaId", List.of(archiveDatabaseResultSearchParameters.getAreaId()));

			if(archiveDatabaseResultSearchParameters.getSourceIds() != null && !archiveDatabaseResultSearchParameters.getSourceIds().isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getSourceIds()));

			if(archiveDatabaseResultSearchParameters.getParameterIds() != null && !archiveDatabaseResultSearchParameters.getParameterIds().isEmpty())
				query.put("parameterId", new ArrayList<>(archiveDatabaseResultSearchParameters.getParameterIds()));

			if(archiveDatabaseResultSearchParameters.getModuleInstanceIds() != null && !archiveDatabaseResultSearchParameters.getModuleInstanceIds().isEmpty())
				query.put("moduleInstanceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getModuleInstanceIds()));

			if(archiveDatabaseResultSearchParameters.getTimeSteps() != null && !archiveDatabaseResultSearchParameters.getTimeSteps().isEmpty())
				query.put("encodedTimeStepId", new ArrayList<>(archiveDatabaseResultSearchParameters.getTimeSteps().stream().map(TimeStep::getEncoded).collect(Collectors.toList())));

			List<String> bucketKeys = List.of("bucketSize", "bucket");
			List<String> collectionKeys = Database.getCollectionKeys(collection);
			List<String> countKeys = collectionKeys.stream().anyMatch(bucketKeys::contains) ? collectionKeys.stream().filter(s -> !bucketKeys.contains(s)).collect(Collectors.toList()) : List.of();

			Map<String, List<String>> distinctKeyFields = Map.of(
					"parameterId", List.of("parameterId"),
					"moduleInstanceId", List.of("moduleInstanceId"),
					"numberOfTimeSeries", countKeys);

			Summarize summarize = (Summarize)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Summarize%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			Map<String, Integer> summary = summarize.getSummary(collection, distinctKeyFields, query, archiveDatabaseResultSearchParameters.getPeriod().getStartDate(), archiveDatabaseResultSearchParameters.getPeriod().getEndDate());

			return new MongoDbArchiveDatabaseSummary(summary.get("parameterId"), summary.get("moduleInstanceId"), summary.get("numberOfTimeSeries"));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param areaId areaId
	 * @param fewsTimeSeriesType fewsTimeSeriesType
	 * @param period period
	 * @param sourceIds sourceIds
	 * @return ArchiveDatabaseFilterOptions
	 */
	@Override
	public ArchiveDatabaseFilterOptions getFilterOptions(String areaId, nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType, Period period, Set<String> sourceIds) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of Period Must Fall On or After Start of Period");

		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			Map<String, Class<?>> fields = Map.of("moduleInstanceId", String.class, "parameterId", String.class, "encodedTimeStepId", String.class);
			Map<String, List<Object>> query = new HashMap<>();
			query.put("metaData.areaId", List.of(areaId));
			if(sourceIds != null && !sourceIds.isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(sourceIds));

			Filter filter = (Filter)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Filter%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			Map<String, List<Object>> filters = filter.getFilters(collection, fields, query, period.getStartDate(), period.getEndDate());
			return new MongoDbArchiveDatabaseFilterOptions(
					filters.get("parameterId").stream().map(Object::toString).sorted().collect(Collectors.toCollection(LinkedHashSet::new)),
					filters.get("moduleInstanceId").stream().map(Object::toString).sorted().collect(Collectors.toCollection(LinkedHashSet::new)),
					filters.get("encodedTimeStepId").stream().map(s -> TimeStepUtils.decode(s.toString())).sorted(Comparator.comparing(Object::toString)).collect(Collectors.toCollection(LinkedHashSet::new))
			);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param fewsTimeSeriesType fewsTimeSeriesType
	 * @return ArchiveDatabaseFilterOptions
	 */
	@Override
	public Set<String> getSourceIds(nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType) {
		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			Set<String> sourceIdsFound = new HashSet<>();
			Database.distinct(collection, "metaData.sourceId", new Document(), String.class).forEach(sourceIdsFound::add);
			return sourceIdsFound;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
