package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import com.mongodb.lang.NonNull;

import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.query.operations.Filter;
import nl.fews.archivedatabase.mongodb.query.operations.HasDataBuckets;
import nl.fews.archivedatabase.mongodb.query.operations.ReadBuckets;
import nl.fews.archivedatabase.mongodb.query.operations.ReadSingletons;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesArrayUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.database.Collection;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;

import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseForecastImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseObservedImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseStitchedSimulatedHistoricalImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.util.*;
import nl.wldelft.util.timeseries.*;

import org.bson.Document;
import org.json.JSONArray;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@SuppressWarnings("unchecked")
public class MongoDbArchiveDatabaseTimeSeriesReader implements ArchiveDatabaseTimeSeriesReader {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(MongoDbArchiveDatabaseTimeSeriesReader.class);

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
	public void setHeaderProvider(@NonNull FewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider) {
		Settings.put("headerProvider", fewsTimeSeriesHeaderProvider);
	}

	/**
	 *
	 * @param archiveDatabaseObservedImportRequests archiveDatabaseObservedImportRequests
	 * @return List<TimeSeriesArrays<TimeSeriesHeader>>
	 */
	@Override
	public List<TimeSeriesArrays<TimeSeriesHeader>> importExternalHistorical(@NonNull Set<ArchiveDatabaseObservedImportRequest> archiveDatabaseObservedImportRequests) {
		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = Collections.synchronizedList(new ArrayList<>());
		archiveDatabaseObservedImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().parallelStream().forEach(fewsTimeSeriesHeader -> {
			TimeSeriesArrays<TimeSeriesHeader> result = importExternalHistorical(archiveDatabaseImportRequest.getPeriod(), fewsTimeSeriesHeader);
			if(!result.isEmpty())
				timeSeriesArrays.add(result);
		}));
		return timeSeriesArrays;
	}

	/**
	 * @param archiveDatabaseStitchedSimulatedHistoricalImportRequest archiveDatabaseStitchedSimulatedHistoricalImportRequest
	 * @return List<TimeSeriesArrays<TimeSeriesHeader>>
	 */
	@Override
	public List<TimeSeriesArrays<TimeSeriesHeader>> importStitchedSimulatedHistorical(@NonNull Set<ArchiveDatabaseStitchedSimulatedHistoricalImportRequest> archiveDatabaseStitchedSimulatedHistoricalImportRequest) {
		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = Collections.synchronizedList(new ArrayList<>());
		archiveDatabaseStitchedSimulatedHistoricalImportRequest.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getTimeSeriesHeaders().parallelStream().forEach(timeSeriesHeader -> {
			for (Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> result:importSimulatedHistoricalStitched(archiveDatabaseImportRequest.getPeriod(), timeSeriesHeader)) {
				if(!result.getObject0().isEmpty())
					timeSeriesArrays.add(result.getObject0());
			}
		}));
		return timeSeriesArrays;
	}

	/**
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getStitchedSimulatedDataImportRequest(@NonNull Period period, @NonNull TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
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
			String moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
			String locationId = timeSeriesHeader.getLocationId();
			String parameterId = timeSeriesHeader.getParameterId();
			String encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();
			String ensembleId = timeSeriesHeader.getEnsembleId() == null || timeSeriesHeader.getEnsembleId().equals("none") || timeSeriesHeader.getEnsembleId().equals("main") ? "" : timeSeriesHeader.getEnsembleId();
			String ensembleMemberId = timeSeriesHeader.getEnsembleMemberId() == null || timeSeriesHeader.getEnsembleMemberId().equals("none") || timeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : timeSeriesHeader.getEnsembleMemberId();

			if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
				logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s",
						moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId));
			}
			else {
				query.put("moduleInstanceId", List.of(moduleInstanceId));
				query.put("locationId", List.of(locationId));
				query.put("parameterId", List.of(parameterId));
				query.put("qualifierId", List.of(qualifierId));
				query.put("encodedTimeStepId", List.of(encodedTimeStepId));
				query.put("ensembleId", List.of(ensembleId));
				query.put("ensembleMemberId", List.of(ensembleMemberId));

				singleExternalDataImportRequests.add(new MongoDbArchiveDatabaseStitchedSimulatedHistoricalImportRequest(List.of(timeSeriesHeader), period, query));
			}
		}
		List<SingleExternalDataImportRequest> singleExternalDataImportRequestsHavingData = Collections.synchronizedList(new ArrayList<>());
		singleExternalDataImportRequests.parallelStream().forEach(singleExternalDataImportRequest -> {
			try{
				MongoDbArchiveDatabaseStitchedSimulatedHistoricalImportRequest archiveDatabaseStitchedSimulatedHistoricalImportRequest = (MongoDbArchiveDatabaseStitchedSimulatedHistoricalImportRequest)singleExternalDataImportRequest;
				String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED);
				Map<String, List<Object>> query = archiveDatabaseStitchedSimulatedHistoricalImportRequest.getQuery();
				Date startDate = archiveDatabaseStitchedSimulatedHistoricalImportRequest.getPeriod().getStartDate();
				Date endDate = archiveDatabaseStitchedSimulatedHistoricalImportRequest.getPeriod().getEndDate();

				if(HasDataBuckets.hasData(collection, query, startDate, endDate))
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
	 * @param period period
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	private TimeSeriesArrays<TimeSeriesHeader> importExternalHistorical(@NonNull Period period, @NonNull TimeSeriesHeader fewsTimeSeriesHeader) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
			qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
		String moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		String locationId = fewsTimeSeriesHeader.getLocationId();
		String parameterId = fewsTimeSeriesHeader.getParameterId();
		String encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId));
		}
		else {
			Map<String, List<Object>> query = new HashMap<>();
			query.put("moduleInstanceId", List.of(moduleInstanceId));
			query.put("locationId", List.of(locationId));
			query.put("parameterId", List.of(parameterId));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(encodedTimeStepId));

			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
			try(MongoCursor<Document> results = new ReadBuckets().read(collection, query, period.getStartDate(), period.getEndDate())) {
				if (results.hasNext()) {
					Document result = results.next();
					Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_HISTORICAL, result);
					timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
				}
			}
		}
		return new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0]));
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedForecasting(@NonNull Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		archiveDatabaseForecastImportRequests.forEach(archiveDatabaseImportRequest -> {
			if(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size() != archiveDatabaseImportRequest.getTaskRunIds().size())
				throw new IllegalArgumentException("archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size() != archiveDatabaseImportRequest.getTaskRunIds().size()");});

		List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> timeSeriesArrays = Collections.synchronizedList(new ArrayList<>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest ->
			IntStream.range(0, archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size()).parallel().forEach(i ->
				timeSeriesArrays.addAll(importSimulatedForecasting(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().get(i), archiveDatabaseImportRequest.getTaskRunIds().get(i)))
			)
		);
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @param taskRunId taskRunId
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedForecasting(@NonNull TimeSeriesHeader fewsTimeSeriesHeader, @NonNull String taskRunId) {
		Map<SystemActivityDescriptor, List<TimeSeriesArray<TimeSeriesHeader>>> timeSeriesArrays = new HashMap<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
			qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
		String moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		String locationId = fewsTimeSeriesHeader.getLocationId();
		String parameterId = fewsTimeSeriesHeader.getParameterId();
		String encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		String ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		String ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s, taskRunId=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId, taskRunId));
		}
		else {
			Map<String, List<Object>> query = new HashMap<>();
			query.put("moduleInstanceId", List.of(moduleInstanceId));
			query.put("locationId", List.of(locationId));
			query.put("parameterId", List.of(parameterId));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(encodedTimeStepId));
			query.put("ensembleId", List.of(ensembleId));
			query.put("ensembleMemberId", List.of(ensembleMemberId));
			query.put("taskRunId", List.of(taskRunId));

			try(MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), query)) {
				if (results.hasNext()) {
					Document result = results.next();
					Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_FORECASTING, result);
					timeSeriesArrays.putIfAbsent(timeSeriesHeader.getObject1(), new ArrayList<>());
					timeSeriesArrays.get(timeSeriesHeader.getObject1()).add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
				}
			}
		}
		return timeSeriesArrays.entrySet().stream().map(k -> new Box<>(new TimeSeriesArrays<TimeSeriesHeader>(k.getValue().toArray(new TimeSeriesArray[0])), k.getKey())).collect(Collectors.toList());
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @param isStitched isStitched
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistorical(@NonNull Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests, boolean isStitched) {
		List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> timeSeriesArrays = Collections.synchronizedList(new ArrayList<>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().parallelStream().forEach(fewsTimeSeriesHeader ->
				timeSeriesArrays.addAll(isStitched ? importSimulatedHistoricalStitched(archiveDatabaseImportRequest.getPeriod(), fewsTimeSeriesHeader) : importSimulatedHistorical(fewsTimeSeriesHeader))));
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistorical(@NonNull TimeSeriesHeader fewsTimeSeriesHeader) {
		Map<SystemActivityDescriptor, List<TimeSeriesArray<TimeSeriesHeader>>> timeSeriesArrays = new HashMap<>();
		Map<String, List<Object>> query = new HashMap<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
			qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
		String moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		String locationId = fewsTimeSeriesHeader.getLocationId();
		String parameterId = fewsTimeSeriesHeader.getParameterId();
		String encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		String ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		String ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();
		Date forecastTime = new Date(fewsTimeSeriesHeader.getForecastTime());

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s, forecastTime=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId, forecastTime));
		}
		else {
			query.put("moduleInstanceId", List.of(moduleInstanceId));
			query.put("locationId", List.of(locationId));
			query.put("parameterId", List.of(parameterId));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(encodedTimeStepId));
			query.put("ensembleId", List.of(ensembleId));
			query.put("ensembleMemberId", List.of(ensembleMemberId));
			query.put("forecastTime", List.of(forecastTime));

			try(MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), query)) {
				if (results.hasNext()) {
					Document result = results.next();
					Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result);
					timeSeriesArrays.putIfAbsent(timeSeriesHeader.getObject1(), new ArrayList<>());
					timeSeriesArrays.get(timeSeriesHeader.getObject1()).add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
				}
			}
		}
		return timeSeriesArrays.entrySet().stream().map(k -> new Box<>(new TimeSeriesArrays<TimeSeriesHeader>(k.getValue().toArray(new TimeSeriesArray[0])), k.getKey())).collect(Collectors.toList());
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @param period period
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistoricalStitched(@NonNull Period period, @NonNull TimeSeriesHeader fewsTimeSeriesHeader) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		Map<SystemActivityDescriptor, List<TimeSeriesArray<TimeSeriesHeader>>> timeSeriesArrays = new HashMap<>();
		Map<String, List<Object>> query = new HashMap<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
			qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
		String moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		String locationId = fewsTimeSeriesHeader.getLocationId();
		String parameterId = fewsTimeSeriesHeader.getParameterId();
		String encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		String ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		String ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId));
		}
		else {
			query.put("moduleInstanceId", List.of(moduleInstanceId));
			query.put("locationId", List.of(locationId));
			query.put("parameterId", List.of(parameterId));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(encodedTimeStepId));
			query.put("ensembleId", List.of(ensembleId));
			query.put("ensembleMemberId", List.of(ensembleMemberId));

			try(MongoCursor<Document> results = new ReadBuckets().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), query, period.getStartDate(), period.getEndDate())) {
				if (results.hasNext()) {
					Document result = results.next();
					Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result);
					timeSeriesArrays.putIfAbsent(timeSeriesHeader.getObject1(), new ArrayList<>());
					timeSeriesArrays.get(timeSeriesHeader.getObject1()).add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
				}
			}
		}
		return timeSeriesArrays.keySet().stream().map(k -> new Box<>(new TimeSeriesArrays<TimeSeriesHeader>(timeSeriesArrays.get(k).toArray(new TimeSeriesArray[0])), k)).collect(Collectors.toList());
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<TimeSeriesArrays<TimeSeriesHeader>> importExternalForecasting(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = Collections.synchronizedList(new ArrayList<>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().parallelStream().forEach(fewsTimeSeriesHeader -> {
			TimeSeriesArrays<TimeSeriesHeader> result = importExternalForecasting(fewsTimeSeriesHeader);
			if(!result.isEmpty())
				timeSeriesArrays.add(result);
		}));
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private TimeSeriesArrays<TimeSeriesHeader> importExternalForecasting(@NonNull TimeSeriesHeader fewsTimeSeriesHeader) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();

		Map<String, List<Object>> query = new HashMap<>();

		List<String> qualifierIds = new ArrayList<>();
		for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
			qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
		String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
		String moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		String locationId = fewsTimeSeriesHeader.getLocationId();
		String parameterId = fewsTimeSeriesHeader.getParameterId();
		String encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		String ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		String ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();
		Date forecastTime = new Date(fewsTimeSeriesHeader.getForecastTime());

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s, forecastTime=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId, forecastTime));
		}
		else {
			query.put("moduleInstanceId", List.of(moduleInstanceId));
			query.put("locationId", List.of(locationId));
			query.put("parameterId", List.of(parameterId));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(encodedTimeStepId));
			query.put("ensembleId", List.of(ensembleId));
			query.put("ensembleMemberId", List.of(ensembleMemberId));
			query.put("forecastTime", List.of(forecastTime));

			try(MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), query)) {
				if (results.hasNext()) {
					Document result = results.next();
					Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_FORECASTING, result);
					timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
				}
			}
		}
		return new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0]));
	}

	/**
	 *
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return List<SingleExternalDataImportRequest>
	 */
	@Override
	public List<SingleExternalDataImportRequest> getExternalHistoricalImportRequest(@NonNull Period period, @NonNull TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
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
			String moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
			String locationId = timeSeriesHeader.getLocationId();
			String parameterId = timeSeriesHeader.getParameterId();
			String encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();

			if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
				logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s",
						moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId));
			}
			else {
				query.put("moduleInstanceId", List.of(moduleInstanceId));
				query.put("locationId", List.of(locationId));
				query.put("parameterId", List.of(parameterId));
				query.put("qualifierId", List.of(qualifierId));
				query.put("encodedTimeStepId", List.of(encodedTimeStepId));

				singleExternalDataImportRequests.add(new MongoDbArchiveDatabaseObservedImportRequest(List.of(timeSeriesHeader), period, query));
			}
		}
		List<SingleExternalDataImportRequest> singleExternalDataImportRequestsHavingData = Collections.synchronizedList(new ArrayList<>());
		singleExternalDataImportRequests.parallelStream().forEach(singleExternalDataImportRequest -> {
			try{
				MongoDbArchiveDatabaseObservedImportRequest archiveDatabaseObservedImportRequest = (MongoDbArchiveDatabaseObservedImportRequest)singleExternalDataImportRequest;
				String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
				Map<String, List<Object>> query = archiveDatabaseObservedImportRequest.getQuery();
				Date startDate = archiveDatabaseObservedImportRequest.getPeriod().getStartDate();
				Date endDate = archiveDatabaseObservedImportRequest.getPeriod().getEndDate();

				if(HasDataBuckets.hasData(collection, query, startDate, endDate))
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
	 * @param timeSeriesHeaders timeSeriesHeaders
	 * @param period period
	 * @return Set<Integer>
	 */
	@Override
	public Set<Integer> getAvailableYears(@NonNull List<TimeSeriesHeader> timeSeriesHeaders, @NonNull Period period) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);

		Set<Integer> availableYears = Collections.synchronizedSet(new HashSet<>());
		timeSeriesHeaders.parallelStream().forEach(timeSeriesHeader -> {
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < timeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(timeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();
			String moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
			String locationId = timeSeriesHeader.getLocationId();
			String parameterId = timeSeriesHeader.getParameterId();
			String encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();

			if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
				logger.info(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s",
						moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId));
			}
			else {
				query.put("moduleInstanceId", List.of(moduleInstanceId));
				query.put("locationId", List.of(locationId));
				query.put("parameterId", List.of(parameterId));
				query.put("qualifierId", List.of(qualifierId));
				query.put("encodedTimeStepId", List.of(encodedTimeStepId));

				LocalDateTime startDate = DateUtil.getLocalDateTime(period.getStartDate());
				LocalDateTime endDate = DateUtil.getLocalDateTime(period.getEndDate());

				Document document = new Document();
				query.forEach((k, v) -> {
					if (!v.isEmpty())
						document.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
				});

				Set<Integer> years = new HashSet<>();
				Database.distinct(collection, "startTime", document, Date.class).forEach(s -> years.add(DateUtil.getLocalDateTime(s).getYear()));
				Database.distinct(collection, "endTime", document, Date.class).forEach(s -> years.add(DateUtil.getLocalDateTime(s).getYear()));

				if(!years.isEmpty()){
					List<Document> yearQueries = new ArrayList<>();
					LongStream.rangeClosed(Collections.min(years), Collections.max(years)).forEach(year -> yearQueries.add(new Document("$and", List.of(
							new Document("endTime", new Document("$gte", DateUtil.getDate(startDate.plusYears(year - startDate.getYear())))),
							new Document("startTime", new Document("$lte", DateUtil.getDate(endDate.plusYears(year - startDate.getYear()))))
					))));
					document.append("$or", yearQueries);

					Database.aggregate(collection, List.of(
							new Document("$match", document),
							new Document("$sort", new Document("startTime", 1).append("endTime", 1)),
							new Document("$group", new Document("_id", new Document("startTime", "$startTime").append("endTime", "$endTime"))))).forEach(result -> {
						Document root = result.get("_id", Document.class);
						int startYear = DateUtil.getLocalDateTime(root.getDate("startTime")).getYear();
						int endYear = DateUtil.getLocalDateTime(root.getDate("endTime")).getYear();
						IntStream.rangeClosed(startYear, endYear).forEach(availableYears::add);
					});
				}
			}
		});
		return availableYears;
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseReadResult
	 */
	@Override
	public ArchiveDatabaseReadResult read(@NonNull ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
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
	public ArchiveDatabaseSummary getSummary(@NonNull ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
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
	public ArchiveDatabaseFilterOptions getFilterOptions(@NonNull String areaId, @NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType, Set<String> sourceIds) {

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
	public Set<String> getSourceIds(@NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType) {
		try{
			TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			Set<String> sourceIdsFound = new HashSet<>();
			Database.distinct(Collection.TimeSeriesIndex.toString(), "sourceId", new Document("collection", collection), String.class).forEach(sourceIdsFound::add);
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
	public Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> getTimeSeriesForTaskRun(@NonNull String taskRunId, @NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
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
	public Set<String> getEnsembleMembers(@NonNull String locationId, @NonNull String parameterId, @NonNull Set<String> moduleInstanceIds, String ensembleId, String[] qualifiers, @NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		Set<String> ensembleMembers = new HashSet<>();
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		Database.distinct(collection, "ensembleMemberId", new Document("locationId", locationId).
				append("parameterId", parameterId).
				append("moduleInstanceId", new Document("$in", new ArrayList<>(moduleInstanceIds))).
				append("ensembleId", ensembleId).
				append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()), String.class).forEach(ensembleMemberId -> {
					if(ensembleMemberId != null && !ensembleMemberId.trim().equals("none") && !ensembleMemberId.trim().equals("0") && !ensembleMemberId.trim().equals(""))
						ensembleMembers.add(ensembleMemberId);
				});
		return ensembleMembers;
	}

	/**
	 *
	 * @param locationId locationId
	 * @param parameterId parameterId
	 * @param ensembleId ensembleId
	 * @param qualifiers qualifiers
	 * @param timeSeriesType timeSeriesType
	 * @return Set<String>
	 */
	@Override
	public Set<String> getModuleInstanceIds(@NonNull String locationId, @NonNull String parameterId, String ensembleId, String[] qualifiers, @NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		Set<String> moduleInstanceIds = new HashSet<>();
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		List<String> collectionKeys = Database.getCollectionKeys(collection);

		Document query = new Document("locationId", locationId);
		query.append("parameterId", parameterId);
		query.append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString());
		if(collectionKeys.contains("ensembleId"))
			query.append("ensembleId", ensembleId);

		Database.distinct(collection, "moduleInstanceId", query, String.class).forEach(moduleInstanceIds::add);
		return moduleInstanceIds;
	}

	/**
	 *
	 * @param locationId locationId
	 * @param parameterId parameterId
	 * @param moduleInstanceId moduleInstanceId
	 * @param ensembleId ensembleId
	 * @param qualifiers qualifiers
	 * @param encodedTimeStepId encodedTimeStepId
	 * @param period period
	 * @param forecastCount forecastCount
	 * @return List<SimulatedTaskRunInfo>
	 */
	@Override
	public List<SimulatedTaskRunInfo> getSimulatedTaskRunInfos(@NonNull String locationId, @NonNull String parameterId, @NonNull String moduleInstanceId, String ensembleId, String[] qualifiers, String encodedTimeStepId, @NonNull Period period, int forecastCount) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		List<SimulatedTaskRunInfo> simulatedTaskRunInfos = new ArrayList<>();

		Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("encodedTimeStepId", encodedTimeStepId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "$taskRunId").append("forecastTime", "$forecastTime").append("dispatchTime", "$runInfo.dispatchTime"))),
				new Document("$sort", new Document("_id.forecastTime", -1).append("_id.taskRunId", 1).append("_id.workflowId", 1).append("_id.dispatchTime", 1)),
				new Document("$limit", forecastCount),
				new Document("$replaceRoot", new Document("newRoot", "$_id")))).forEach(result ->
				simulatedTaskRunInfos.add(new SimulatedTaskRunInfo(result.getString("workflowId"), result.getString("taskRunId"), result.getDate("forecastTime").getTime(), result.getDate("dispatchTime").getTime())));

		Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("encodedTimeStepId", encodedTimeStepId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "$taskRunId").append("forecastTime", "$forecastTime").append("dispatchTime", "$runInfo.dispatchTime"))),
				new Document("$sort", new Document("_id.forecastTime", -1).append("_id.taskRunId", 1).append("_id.workflowId", 1).append("_id.dispatchTime", 1)),
				new Document("$limit", forecastCount),
				new Document("$replaceRoot", new Document("newRoot", "$_id")))).forEach(result ->
				simulatedTaskRunInfos.add(new SimulatedTaskRunInfo(result.getString("workflowId"), result.getString("taskRunId"), result.getDate("forecastTime").getTime(), result.getDate("dispatchTime").getTime())));

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
	public LongUnmodifiableList searchForExternalForecastTimes(@NonNull String locationId, @NonNull String parameterId, @NonNull String moduleInstanceId, String ensembleId, String[] qualifiers, @NonNull nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType, Period period, int forecastCount) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		LongListBuilder longListBuilder = new LongListBuilder();

		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		Database.aggregate(collection, List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$group", new Document("_id", "$forecastTime")),
				new Document("$sort", new Document("_id", -1)),
				new Document("$limit", forecastCount))).forEach(result ->
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
