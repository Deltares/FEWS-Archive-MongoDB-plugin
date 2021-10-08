package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.HasData;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.query.operations.Filter;
import nl.fews.archivedatabase.mongodb.query.utils.HeaderRequestUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeaders;
import nl.wldelft.util.LongUnmodifiableList;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.*;
import org.bson.Document;
import org.json.JSONArray;

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
	public TimeSeriesArrays<TimeSeriesHeader> importSingleDataImportRequest(SingleExternalDataImportRequest singleExternalDataImportRequest) {
		try {
			MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
			Read read = (Read)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Read%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType())))).getConstructor().newInstance();
			MongoCursor<Document> results = read.read(
					TimeSeriesTypeUtil.getTimeSeriesTypeCollection(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType()),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery(),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate(),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate());
			TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesArray();
			if(results.hasNext()){
				Document result = results.next();

				Map<Long, Float> resultMap = result.getList("timeseries", Document.class).stream().collect(Collectors.toMap(s -> s.getDate("t").getTime(), s -> s.get("v") != null ? s.getDouble("v").floatValue() : Float.NaN));
				for (int i = 0; i < timeSeriesArray.size(); i++) {
					long time = timeSeriesArray.getTime(i);
					if(timeSeriesArray.isValueReliable(i) && resultMap.containsKey(time)) {
						timeSeriesArray.setValue(i, resultMap.get(time));
					}
				}
			}
			return new TimeSeriesArrays<>(timeSeriesArray);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
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
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = new ArrayList<>();
		for (int i = 0; i < timeSeriesArrays.size(); i++) {
			TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = timeSeriesArrays.get(i);
			if(!timeSeriesArray.isCompletelyReliable() || timeSeriesArray.getHeader().getTimeStep() == IrregularTimeStep.INSTANCE) {
				TimeSeriesHeader timeSeriesHeader = timeSeriesArray.getHeader();
				Map<String, List<Object>> query = new HashMap<>();

				List<String> qualifierIds = new ArrayList<>();
				for (int j = 0; j < timeSeriesHeader.getQualifierCount(); j++)
					qualifierIds.add(timeSeriesHeader.getQualifierId(j));
				String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

				query.put("moduleInstanceId", List.of(timeSeriesHeader.getModuleInstanceId()));
				query.put("locationId", List.of(timeSeriesHeader.getLocationId()));
				query.put("parameterId", List.of(timeSeriesHeader.getParameterId()));
				query.put("qualifierId", List.of(qualifierId));
				query.put("encodedTimeStepId", List.of(timeSeriesHeader.getTimeStep().getEncoded()));

				singleExternalDataImportRequests.add(new MongoDbArchiveDatabaseSingleExternalImportRequest(period, TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, query, timeSeriesArray));
			}
		}
		List<SingleExternalDataImportRequest> singleExternalDataImportRequestsHavingData = new ArrayList<>();
		singleExternalDataImportRequests.parallelStream().forEach(singleExternalDataImportRequest -> {
			try{
				MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
				HasData hasData = (HasData)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("HasData%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType())))).getConstructor().newInstance();
				if(hasData.hasData(
						TimeSeriesTypeUtil.getTimeSeriesTypeCollection(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType()),
						mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery(),
						mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate(),
						mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate()
				)){
					singleExternalDataImportRequestsHavingData.add(singleExternalDataImportRequest);
				}
			}
			catch (Exception ex){
				throw new RuntimeException(ex);
			}
		});
		return singleExternalDataImportRequestsHavingData;
	}

	/**
	 *
	 * @param mongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest
	 * @return boolean
	 */
	private boolean singleDataImportRequestHasData(MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest) {
		try {
			HasData hasData = (HasData)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("HasData%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType())))).getConstructor().newInstance();
			return hasData.hasData(
					TimeSeriesTypeUtil.getTimeSeriesTypeCollection(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType()),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery(),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate(),
					mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate()
			);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseReadResult
	 */
	@Override
	public ArchiveDatabaseReadResult read(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		if(archiveDatabaseResultSearchParameters.getPeriod().getEndDate().before(archiveDatabaseResultSearchParameters.getPeriod().getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

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
			throw new IllegalArgumentException("End of period must fall on or after start of period");

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
	 * @param sourceIds sourceIds
	 * @return ArchiveDatabaseFilterOptions
	 */
	@Override
	public ArchiveDatabaseFilterOptions getFilterOptions(String areaId, nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType, Set<String> sourceIds) {

		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			Map<String, Class<?>> fields = Map.of("moduleInstanceId", String.class, "parameterId", String.class, "encodedTimeStepId", String.class);
			Map<String, List<Object>> query = new HashMap<>();
			query.put("metaData.areaId", List.of(areaId));
			if(sourceIds != null && !sourceIds.isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(sourceIds));

			Map<String, List<Object>> filters = Filter.getFilters(collection, fields, query);
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
			Database.distinct(Database.Collection.TimeSeriesIndex.toString(), "sourceId", new Document("collection", collection), String.class).forEach(sourceIdsFound::add);
			return sourceIdsFound;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param taskRunId taskRunId
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<TimeSeriesHeader> getTimeSeriesForTaskRun(String taskRunId) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();

		Database.find(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), new Document("taskRunId", taskRunId)).forEach(result ->
				timeSeriesArrays.add(HeaderRequestUtil.getTimeSeriesArray(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result)));

		Database.find(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), new Document("taskRunId", taskRunId)).forEach(result ->
				timeSeriesArrays.add(HeaderRequestUtil.getTimeSeriesArray(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_FORECASTING, result)));

		return new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0]));
	}

	@Override
	public Set<String> getEnsembleMembers(String locationId, String parameterId, Set<String> moduleInstanceId, String ensembleId, String[] qualifiers, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		Database.find(collection, new Document("locationId", locationId).
				append("parameterId", parameterId).
				append("moduleInstanceId", new Document("$in", new ArrayList<>(moduleInstanceId))).
				append("ensembleId", ensembleId).
				append("qualifiers", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()));
		return null;
	}

	@Override
	public List<SimulatedTaskRunInfo> getSimulatedTaskRunInfos(String s, String s1, String s2, String s3, String[] strings, Period period, int i) {
		return null;
	}

	@Override
	public LongUnmodifiableList searchForExternalForecastTimes(String s, String s1, String s2, String s3, String[] strings, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType, Period period, int i) {
		return null;
	}

	@Override
	public List<SingleExternalDataImportRequest> getExternalForecastImportRequests(FewsTimeSeriesHeaders fewsTimeSeriesHeaders) {
		return null;
	}
}
