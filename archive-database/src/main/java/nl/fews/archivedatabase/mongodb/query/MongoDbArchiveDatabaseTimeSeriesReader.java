package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.HasData;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.query.operations.Filter;
import nl.fews.archivedatabase.mongodb.query.utils.TimeSeriesArrayUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeaders;
import nl.wldelft.util.*;
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
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesValueType(), mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType());
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			Map<String, List<Object>> query = mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery();
			Date startDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod() == null ? null : mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate();
			Date endDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod() == null ? null : mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate();

			Read read = (Read)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Read%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			MongoCursor<Document> results = read.read(collection, query, startDate, endDate);

			if(results.hasNext()){
				Document result = results.next();
				TimeSeriesArray<TimeSeriesHeader> requestTimeSeriesArray = mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesArray();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesValueType(), mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType(), result);

				return new TimeSeriesArrays<>(requestTimeSeriesArray != null ?
						TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class), requestTimeSeriesArray) :
						TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
			}
			return null;
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

			singleExternalDataImportRequests.add(new MongoDbArchiveDatabaseSingleExternalImportRequest(period, query, TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_HISTORICAL, timeSeriesArray));
		}
		List<SingleExternalDataImportRequest> singleExternalDataImportRequestsHavingData = new ArrayList<>();
		singleExternalDataImportRequests.parallelStream().forEach(singleExternalDataImportRequest -> {
			try{
				MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
				TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesValueType(), mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType());
				String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
				Map<String, List<Object>> query = mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery();
				Date startDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate();
				Date endDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate();

				HasData hasData = (HasData)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("HasData%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
				if(hasData.hasData(collection, query, startDate, endDate))
					singleExternalDataImportRequestsHavingData.add(singleExternalDataImportRequest);
			}
			catch (Exception ex){
				throw new RuntimeException(ex);
			}
		});
		return singleExternalDataImportRequestsHavingData;
	}


	/**
	 *
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getExternalForecastImportRequests(FewsTimeSeriesHeaders fewsTimeSeriesHeaders) {
		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = new ArrayList<>();
		for (int i = 0; i < fewsTimeSeriesHeaders.size(); i++) {
			FewsTimeSeriesHeader fewsTimeSeriesHeader = fewsTimeSeriesHeaders.get(i);
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
			String ensembleId = fewsTimeSeriesHeader.getEnsembleId() != null && !fewsTimeSeriesHeader.getEnsembleId().equals("none") ? fewsTimeSeriesHeader.getEnsembleId() : "";
			String ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() != null && !fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") ? fewsTimeSeriesHeader.getEnsembleMemberId() : "";
			Date forecastTime = new Date(fewsTimeSeriesHeader.getForecastTime());

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));
			query.put("ensembleId", List.of(ensembleId));
			query.put("ensembleMemberId", List.of(ensembleMemberId));
			query.put("forecastTime", List.of(forecastTime));

			singleExternalDataImportRequests.add(new MongoDbArchiveDatabaseSingleExternalImportRequest(null, query, TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_FORECASTING, null));
		}
		List<SingleExternalDataImportRequest> singleExternalDataImportRequestsHavingData = new ArrayList<>();
		singleExternalDataImportRequests.parallelStream().forEach(singleExternalDataImportRequest -> {
			try{
				MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
				TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesValueType(), mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType());
				String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
				Map<String, List<Object>> query = mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery();

				HasData hasData = (HasData)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("HasData%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
				if(hasData.hasData(collection, query,null, null))
					singleExternalDataImportRequestsHavingData.add(singleExternalDataImportRequest);
			}
			catch (Exception ex){
				throw new RuntimeException(ex);
			}
		});
		return singleExternalDataImportRequestsHavingData;
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
	 * @param timeSeriesType timeSeriesType
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override
	public Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> getTimeSeriesForTaskRun(String taskRunId, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		List<SystemActivityDescriptor> systemActivityDescriptors = new ArrayList<>();

		Database.find(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType)), new Document("taskRunId", taskRunId)).forEach(result -> {
			Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, timeSeriesType, result);
			systemActivityDescriptors.add(timeSeriesHeader.getObject1());
			TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class));
			timeSeriesArrays.add(timeSeriesArray);
		});

		return new Box<>(new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0])), systemActivityDescriptors.stream().findFirst().orElse(null));
	}

	/**
	 *
	 * @param locationId locationId
	 * @param parameterId parameterId
	 * @param moduleInstanceIds moduleInstanceIds
	 * @param ensembleId ensembleId
	 * @param qualifiers qualifiers
	 * @param timeSeriesType timeSeriesType
	 * @return Set<String>
	 */
	@Override
	public Set<String> getEnsembleMembers(String locationId, String parameterId, Set<String> moduleInstanceIds, String ensembleId, String[] qualifiers, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		Set<String> ensembleMembers = new HashSet<>();
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		Database.distinct(collection, "ensembleMemberId", new Document("locationId", locationId).
				append("parameterId", parameterId).
				append("moduleInstanceId", new Document("$in", new ArrayList<>(moduleInstanceIds))).
				append("ensembleId", ensembleId).
				append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()), String.class).forEach(ensembleMemberId -> {
					if(ensembleMemberId != null && !ensembleMemberId.trim().equals(""))
						ensembleMembers.add(ensembleMemberId);
				});
		return ensembleMembers;
	}

	/**
	 *
	 * @param locationId locationId
	 * @param parameterId parameterId
	 * @param moduleInstanceId moduleInstanceId
	 * @param ensembleId ensembleId
	 * @param qualifiers qualifiers
	 * @param period period
	 * @param forecastCount forecastCount
	 * @return List<SimulatedTaskRunInfo>
	 */
	@Override
	public List<SimulatedTaskRunInfo> getSimulatedTaskRunInfos(String locationId, String parameterId, String moduleInstanceId, String ensembleId, String[] qualifiers, Period period, int forecastCount) {
		List<SimulatedTaskRunInfo> simulatedTaskRunInfos = new ArrayList<>();

		Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$limit", forecastCount),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "$runInfo.taskRunId").append("time0", "$runInfo.time0").append("dispatchTime", "$runInfo.dispatchTime"))),
				new Document("$replaceRoot", new Document("newRoot", "$_id")))).forEach(result ->
				simulatedTaskRunInfos.add(new SimulatedTaskRunInfo(result.getString("_id.workflowId"), result.getString("_id.taskRunId"), result.getDate("_id.time0").getTime(), result.getDate("_id.dispatchTime").getTime())));

		Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$limit", forecastCount),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "$runInfo.taskRunId").append("time0", "$runInfo.time0").append("dispatchTime", "$runInfo.dispatchTime"))),
				new Document("$replaceRoot", new Document("newRoot", "$_id")))).forEach(result ->
				simulatedTaskRunInfos.add(new SimulatedTaskRunInfo(result.getString("workflowId"), result.getString("taskRunId"), result.getDate("time0").getTime(), result.getDate("dispatchTime").getTime())));

		return simulatedTaskRunInfos;
	}

	/**
	 *
	 * @param locationId locationId
	 * @param parameterId parameterId
	 * @param moduleInstanceId moduleInstanceId
	 * @param ensembleId ensembleId
	 * @param qualifiers qualifiers
	 * @param timeSeriesType timeSeriesType
	 * @param period period
	 * @param forecastCount forecastCount
	 * @return LongUnmodifiableList
	 */
	@Override
	public LongUnmodifiableList searchForExternalForecastTimes(String locationId, String parameterId, String moduleInstanceId, String ensembleId, String[] qualifiers, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType, Period period, int forecastCount) {
		LongListBuilder longListBuilder = new LongListBuilder();

		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		Database.aggregate(collection, List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$limit", forecastCount),
				new Document("$group", new Document("_id", "$forecastTime")))).forEach(result ->
				longListBuilder.add(result.getDate("_id").getTime()));

		return longListBuilder.build();
	}
}

/**
 *
 */
class LongListBuilder extends LongArrayOrListBuilder {
	/**
	 *
	 */
	LongListBuilder() {
	}

	/**
	 *
	 * @return LongUnmodifiableList
	 */
	LongUnmodifiableList build() {
		return super.buildList();
	}
}
