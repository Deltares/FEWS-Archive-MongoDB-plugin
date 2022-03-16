package nl.fews.archivedatabase.mongodb.query;

import com.mongodb.client.MongoCursor;
import com.mongodb.lang.NonNull;

import nl.fews.archivedatabase.mongodb.query.interfaces.HasData;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.query.interfaces.Summarize;
import nl.fews.archivedatabase.mongodb.query.operations.Filter;
import nl.fews.archivedatabase.mongodb.query.operations.ReadBuckets;
import nl.fews.archivedatabase.mongodb.query.operations.ReadSingletons;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesArrayUtil;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.fews.archivedatabase.mongodb.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.shared.settings.Settings;
import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.TimeSeriesTypeUtil;

import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseForecastImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.ArchiveDatabaseObservedImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.util.*;
import nl.wldelft.util.timeseries.*;

import org.bson.Document;
import org.json.JSONArray;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
@SuppressWarnings("unchecked")
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
	public void setHeaderProvider(@NonNull FewsTimeSeriesHeaderProvider fewsTimeSeriesHeaderProvider) {
		Settings.put("headerProvider", fewsTimeSeriesHeaderProvider);
	}

	@Override
	public List<TimeSeriesArrays<TimeSeriesHeader>> importObservedData(Set<ArchiveDatabaseObservedImportRequest> archiveDatabaseObservedImportRequest) {
		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		archiveDatabaseObservedImportRequest.parallelStream().forEach(archiveDatabaseImportRequest -> {
			TimeSeriesArrays<TimeSeriesHeader> result = importObservedData(archiveDatabaseImportRequest.getPeriod(), archiveDatabaseImportRequest.getFewsTimeSeriesHeaders());
			if(!result.isEmpty())
				timeSeriesArrays.add(result);
		});
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param period period
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	private TimeSeriesArrays<TimeSeriesHeader> importObservedData(@NonNull Period period, @NonNull List<TimeSeriesHeader> fewsTimeSeriesHeaders) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		fewsTimeSeriesHeaders.parallelStream().forEach(fewsTimeSeriesHeader -> {
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));

			MongoCursor<Document> results = new ReadBuckets().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), query, period.getStartDate(), period.getEndDate());
			if (results.hasNext()) {
				Document result = results.next();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_HISTORICAL, result);
				timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
			}
		});
		return new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0]));
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedForecasting(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> timeSeriesArrays = new ArrayList<>();
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> {
			Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> result = importSimulatedForecasting(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders(), archiveDatabaseImportRequest.getTaskRunIds());
			if(!result.getObject0().isEmpty())
				timeSeriesArrays.add(result);
		});
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> importSimulatedForecasting(@NonNull List<TimeSeriesHeader> fewsTimeSeriesHeaders, List<String> taskRunIds) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		List<SystemActivityDescriptor> systemActivityDescriptors = new ArrayList<>();

		if(fewsTimeSeriesHeaders.size() != taskRunIds.size())
			throw new IllegalArgumentException("fewsTimeSeriesHeaders.size() != taskRunIds.size()");

		AtomicInteger index = new AtomicInteger();
		fewsTimeSeriesHeaders.parallelStream().forEach(fewsTimeSeriesHeader -> {
			String taskRunId = taskRunIds.get(index.getAndIncrement());
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));
			query.put("ensembleId", List.of(fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId()));
			query.put("ensembleMemberId", List.of(fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId()));
			query.put("forecastTime", List.of(new Date(fewsTimeSeriesHeader.getForecastTime())));
			query.put("taskRunId", List.of(taskRunId));

			MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), query);
			if (results.hasNext()) {
				Document result = results.next();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_FORECASTING, result);
				systemActivityDescriptors.add(timeSeriesHeader.getObject1());
				timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
			}
		});
		return new Box<>(new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0])),  systemActivityDescriptors.stream().findFirst().orElse(null));
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @param isStitched isStitched
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistorical(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests, boolean isStitched) {
		List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>> timeSeriesArrays = new ArrayList<>();
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> {
			Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> result = isStitched ? importSimulatedHistoricalStitched(archiveDatabaseImportRequest.getPeriod(), archiveDatabaseImportRequest.getFewsTimeSeriesHeaders()) : importSimulatedHistorical(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders());
			if(!result.getObject0().isEmpty())
				timeSeriesArrays.add(result);
		});
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> importSimulatedHistorical(@NonNull List<TimeSeriesHeader> fewsTimeSeriesHeaders) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		List<SystemActivityDescriptor> systemActivityDescriptors = new ArrayList<>();

		fewsTimeSeriesHeaders.parallelStream().forEach(fewsTimeSeriesHeader -> {
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));
			query.put("ensembleId", List.of(fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId()));
			query.put("ensembleMemberId", List.of(fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId()));
			query.put("forecastTime", List.of(new Date(fewsTimeSeriesHeader.getForecastTime())));

			MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL), query);
			if (results.hasNext()) {
				Document result = results.next();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result);
				systemActivityDescriptors.add(timeSeriesHeader.getObject1());
				timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
			}
		});
		return new Box<>(new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0])),  systemActivityDescriptors.stream().findFirst().orElse(null));
	}

	/**
	 *
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @param period period
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor> importSimulatedHistoricalStitched(@NonNull Period period, @NonNull List<TimeSeriesHeader> fewsTimeSeriesHeaders) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		List<SystemActivityDescriptor> systemActivityDescriptors = new ArrayList<>();

		fewsTimeSeriesHeaders.parallelStream().forEach(fewsTimeSeriesHeader -> {
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));
			query.put("ensembleId", List.of(fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId()));
			query.put("ensembleMemberId", List.of(fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId()));

			MongoCursor<Document> results = new ReadBuckets().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), query, period.getStartDate(), period.getEndDate());
			if (results.hasNext()) {
				Document result = results.next();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result);
				systemActivityDescriptors.add(timeSeriesHeader.getObject1());
				timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
			}
		});
		return new Box<>(new TimeSeriesArrays<>(timeSeriesArrays.toArray(new TimeSeriesArray[0])),  systemActivityDescriptors.stream().findFirst().orElse(null));
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<TimeSeriesArrays<TimeSeriesHeader>> importExternalForecasting(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		List<TimeSeriesArrays<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> {
			TimeSeriesArrays<TimeSeriesHeader> result = importExternalForecasting(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders());
			if(!result.isEmpty())
				timeSeriesArrays.add(result);
		});
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeaders fewsTimeSeriesHeaders
	 * @return Box<TimeSeriesArrays<TimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private TimeSeriesArrays<TimeSeriesHeader> importExternalForecasting(@NonNull List<TimeSeriesHeader> fewsTimeSeriesHeaders) {
		List<TimeSeriesArray<TimeSeriesHeader>> timeSeriesArrays = new ArrayList<>();

		for (TimeSeriesHeader fewsTimeSeriesHeader : fewsTimeSeriesHeaders) {
			Map<String, List<Object>> query = new HashMap<>();

			List<String> qualifierIds = new ArrayList<>();
			for (int j = 0; j < fewsTimeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(fewsTimeSeriesHeader.getQualifierId(j));
			String qualifierId = new JSONArray(qualifierIds.stream().sorted().collect(Collectors.toList())).toString();

			query.put("moduleInstanceId", List.of(fewsTimeSeriesHeader.getModuleInstanceId()));
			query.put("locationId", List.of(fewsTimeSeriesHeader.getLocationId()));
			query.put("parameterId", List.of(fewsTimeSeriesHeader.getParameterId()));
			query.put("qualifierId", List.of(qualifierId));
			query.put("encodedTimeStepId", List.of(fewsTimeSeriesHeader.getTimeStep().getEncoded()));
			query.put("ensembleId", List.of(fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId()));
			query.put("ensembleMemberId", List.of(fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId()));
			query.put("forecastTime", List.of(new Date(fewsTimeSeriesHeader.getForecastTime())));

			MongoCursor<Document> results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_FORECASTING), query);
			if (results.hasNext()) {
				Document result = results.next();
				Box<TimeSeriesHeader, SystemActivityDescriptor> timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_FORECASTING, result);
				timeSeriesArrays.add(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), result.getList("timeseries", Document.class)));
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
	@Override @Deprecated(since = "2022-03-12", forRemoval = true)
	public List<SingleExternalDataImportRequest> getObservedDataImportRequest(@NonNull Period period, @NonNull TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays) {
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
	 * @param singleExternalDataImportRequest singleExternalDataImportRequest
	 * @return TimeSeriesArrays<TimeSeriesHeader>
	 */
	@Override @Deprecated(since = "2022-03-12", forRemoval = true)
	public TimeSeriesArrays<TimeSeriesHeader> importSingleObservedDataImportRequest(SingleExternalDataImportRequest singleExternalDataImportRequest) {
		MongoDbArchiveDatabaseSingleExternalImportRequest mongoDbArchiveDatabaseSingleExternalImportRequest = (MongoDbArchiveDatabaseSingleExternalImportRequest)singleExternalDataImportRequest;
		TimeSeriesType timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesValueType(), mongoDbArchiveDatabaseSingleExternalImportRequest.getTimeSeriesType());
		String collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
		Map<String, List<Object>> query = mongoDbArchiveDatabaseSingleExternalImportRequest.getQuery();
		Date startDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod() == null ? null : mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getStartDate();
		Date endDate = mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod() == null ? null : mongoDbArchiveDatabaseSingleExternalImportRequest.getPeriod().getEndDate();

		MongoCursor<Document> results = new ReadBuckets().read(collection, query, startDate, endDate);
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

		Set<Integer> availableYears = new HashSet<>();
		timeSeriesHeaders.parallelStream().forEach(timeSeriesHeader -> {
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

			Date startDate = period.getStartDate();
			Date endDate = period.getEndDate();

			Document document = new Document();
			document.append("endTime", new Document("$gte", startDate));
			document.append("startTime", new Document("$lte", endDate));
			query.forEach((k, v) -> {
				if(!v.isEmpty())
					document.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
			});
			Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), List.of(
					new Document("$sort", new Document("startTime", 1).append("endTime", 1)),
					new Document("$group", new Document("_id", new Document("startTime", "$startTime").append("endTime", "$endTime"))))).forEach(result -> {
				Document root = result.get("_id", Document.class);
				int startYear = DateUtil.getLocalDateTime(root.getDate("startDate")).getYear();
				int endYear = DateUtil.getLocalDateTime(root.getDate("endDate")).getYear();
				IntStream.rangeClosed(startYear, endYear).forEach(availableYears::add);
			});
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
				new Document("$limit", forecastCount),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "taskRunId").append("forecastTime", "$forecastTime").append("dispatchTime", "$runInfo.dispatchTime"))),
				new Document("$replaceRoot", new Document("newRoot", "$_id")))).forEach(result ->
				simulatedTaskRunInfos.add(new SimulatedTaskRunInfo(result.getString("workflowId"), result.getString("taskRunId"), result.getDate("forecastTime").getTime(), result.getDate("dispatchTime").getTime())));

		Database.aggregate(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), List.of(
				new Document("$match", new Document("locationId", locationId).append("parameterId", parameterId).append("moduleInstanceId", moduleInstanceId).append("ensembleId", ensembleId).append("encodedTimeStepId", encodedTimeStepId).append("qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().collect(Collectors.toList())).toString()).append("forecastTime", new Document("$gte", period.getStartDate()).append("$lte", period.getEndDate()))),
				new Document("$limit", forecastCount),
				new Document("$group", new Document("_id", new Document("workflowId", "$runInfo.workflowId").append("taskRunId", "taskRunId").append("forecastTime", "$forecastTime").append("dispatchTime", "$runInfo.dispatchTime"))),
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
