package nl.fews.archivedatabase.common.query;

import nl.fews.archivedatabase.common.query.operations.ReadBuckets;
import nl.fews.archivedatabase.common.query.operations.ReadSingletons;
import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesArrayUtil;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.utils.DateUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;

import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.fews.archivedatabase.interfaces.ArchiveDatabaseForecastImportRequest;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.*;
import nl.wldelft.util.timeseries.*;

import org.json.JSONArray;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@SuppressWarnings({"unchecked"})
public class ArchiveDatabaseTimeSeriesReader implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseTimeSeriesReader, AutoCloseable {

	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(ArchiveDatabaseTimeSeriesReader.class);

	/**
	 *
	 */
	private static ArchiveDatabaseTimeSeriesReader archiveDatabaseTimeSeriesReader = null;

	/**
	 *
	 */
	private static final Object mutex = new Object();
	
	/**
	 *
	 */
	private final DatabaseBridge database;
	
	/**
	 * block direct instantiation; use static create() method
	 */
	private ArchiveDatabaseTimeSeriesReader(){
		try{
			database = (DatabaseBridge)Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static ArchiveDatabaseTimeSeriesReader create() {
		if(archiveDatabaseTimeSeriesReader == null)
			archiveDatabaseTimeSeriesReader = new ArchiveDatabaseTimeSeriesReader();
		return archiveDatabaseTimeSeriesReader;
	}

	public void close() {
		synchronized (mutex) {
			ArchiveDatabaseTimeSeriesReader.archiveDatabaseTimeSeriesReader = null;
			database.close();
		}
	}

	/**
	 * @param period period
	 * @param timeSeriesArrays timeSeriesArrays
	 * @return TimeSeriesArrays<FewsTimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<FewsTimeSeriesHeader> importStitchedSimulatedHistorical(Period period, TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		var  timeSeriesArraysResults = Collections.synchronizedList(new ArrayList<TimeSeriesArray<FewsTimeSeriesHeader>>());
		IntStream.range(0, timeSeriesArrays.size()).parallel().forEach(i -> {
			var timeSeriesArray = timeSeriesArrays.get(i);
			var timeSeriesArrayResult = importStitchedSimulatedHistorical(period, timeSeriesArray.getHeader());
			if (timeSeriesArrayResult != null && timeSeriesArrayResult.getObject0().getHeader() != null && !timeSeriesArrayResult.getObject0().getHeader().equals(FewsTimeSeriesHeader.NONE) && !timeSeriesArrayResult.getObject0().getHeader().isNone() && !TimeSeriesArrayUtil.timeSeriesArrayValuesAreEqual(timeSeriesArray, timeSeriesArrayResult.getObject0()))
				timeSeriesArraysResults.add(timeSeriesArrayResult.getObject0());
		});
		return new TimeSeriesArrays<FewsTimeSeriesHeader>(timeSeriesArraysResults.toArray(new TimeSeriesArray[0]));
	}

	/**
	 * @param period period
	 * @param timeSeriesHeader timeSeriesHeader
	 * @return TimeSeriesArray<FewsTimeSeriesHeader>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importStitchedSimulatedHistorical(Period period, FewsTimeSeriesHeader timeSeriesHeader) {
		var qualifierIds = IntStream.range(0, timeSeriesHeader.getQualifierCount()).mapToObj(timeSeriesHeader::getQualifierId).toList();
		var qualifierId = new JSONArray(qualifierIds.stream().sorted().toList()).toString();
		var moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
		var locationId = timeSeriesHeader.getLocationId();
		var parameterId = timeSeriesHeader.getParameterId();
		var encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();
		var ensembleId = timeSeriesHeader.getEnsembleId() == null || timeSeriesHeader.getEnsembleId().equals("none") || timeSeriesHeader.getEnsembleId().equals("main") ? "" : timeSeriesHeader.getEnsembleId();
		var ensembleMemberId = timeSeriesHeader.getEnsembleMemberId() == null || timeSeriesHeader.getEnsembleMemberId().equals("none") || timeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : timeSeriesHeader.getEnsembleMemberId();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.debug(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s",
				moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId));
		}
		else {
			var query = new LinkedHashMap<String, Object>(Map.of(
			"moduleInstanceId", moduleInstanceId,
			"locationId", locationId,
			"parameterId", parameterId,
			"qualifierId", qualifierId,
			"encodedTimeStepId", encodedTimeStepId,
			"ensembleId", ensembleId,
			"ensembleMemberId", ensembleMemberId));

			try(var results = new ReadBuckets().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED), query, period.getStartDate(), period.getEndDate()).iterator()) {
				if (results.hasNext()) {
					var result = results.next();
					var timeSeriesHeaderResult = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL, result);
					return new Box<>(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeaderResult.getObject0(), (List<Map<String, Object>>)result.get("timeseries")), timeSeriesHeaderResult.getObject1());
				}
			}
		}
		return null;
	}

	/**
	 *
	 * @param period period
	 * @param  timeSeriesArrays timeSeriesArrays
	 * @return TimeSeriesArrays<FewsTimeSeriesHeader>
	 */
	@Override
	public TimeSeriesArrays<FewsTimeSeriesHeader> importExternalHistorical(Period period, TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");
		var timeSeriesArraysResults = Collections.synchronizedList(new ArrayList<TimeSeriesArray<FewsTimeSeriesHeader>>());
		IntStream.range(0, timeSeriesArrays.size()).parallel().forEach(i -> {
			var timeSeriesArray = timeSeriesArrays.get(i);
			var timeSeriesArrayResult = importExternalHistorical(period, timeSeriesArray.getHeader());
			if (timeSeriesArrayResult != null && timeSeriesArrayResult.getObject0().getHeader() != null && !timeSeriesArrayResult.getObject0().getHeader().equals(FewsTimeSeriesHeader.NONE) && !timeSeriesArrayResult.getObject0().getHeader().isNone() && !TimeSeriesArrayUtil.timeSeriesArrayValuesAreEqual(timeSeriesArray, timeSeriesArrayResult.getObject0()))
				timeSeriesArraysResults.add(timeSeriesArrayResult.getObject0());
		});
		return new TimeSeriesArrays<FewsTimeSeriesHeader>(timeSeriesArraysResults.toArray(new TimeSeriesArray[0]));
	}

	/**
	 *
	 * @param period period
	 * @param  timeSeriesHeader timeSeriesHeader
	 * @return TimeSeriesArray<FewsTimeSeriesHeader>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importExternalHistorical(Period period, FewsTimeSeriesHeader timeSeriesHeader) {
		var qualifierIds = IntStream.range(0, timeSeriesHeader.getQualifierCount()).mapToObj(timeSeriesHeader::getQualifierId).toList();
		var qualifierId = new JSONArray(qualifierIds.stream().sorted().toList()).toString();
		var moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
		var locationId = timeSeriesHeader.getLocationId();
		var parameterId = timeSeriesHeader.getParameterId();
		var encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.debug(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s",
				moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId));
		}
		else {
			var query = new LinkedHashMap<String, Object>(Map.of(
			"moduleInstanceId", moduleInstanceId,
			"locationId", locationId,
			"parameterId", parameterId,
			"qualifierId", qualifierId,
			"encodedTimeStepId", encodedTimeStepId));

			try(var results = new ReadBuckets().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL), query, period.getStartDate(), period.getEndDate()).iterator()) {
				if (results.hasNext()) {
					var result = results.next();
					var timeSeriesHeaderResult = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_HISTORICAL, result);
					return new Box<>(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeaderResult.getObject0(), (List<Map<String, Object>>)result.get("timeseries")), timeSeriesHeaderResult.getObject1());
				}
			}
		}
		return null;
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @param isStitched isStitched
	 * @return List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistorical(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests, boolean isStitched) {
		var timeSeriesArrays = Collections.synchronizedList(new ArrayList<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().parallelStream().forEach(fewsTimeSeriesHeader -> {
			Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> timeSeriesArrayResult = isStitched ? importStitchedSimulatedHistorical(archiveDatabaseImportRequest.period(), fewsTimeSeriesHeader) : importSimulatedHistorical(fewsTimeSeriesHeader);
			if(timeSeriesArrayResult != null && timeSeriesArrayResult.getObject0().getHeader() != null && !timeSeriesArrayResult.getObject0().getHeader().equals(FewsTimeSeriesHeader.NONE) && !timeSeriesArrayResult.getObject0().getHeader().isNone())
				timeSeriesArrays.add(new Box<>(new TimeSeriesArrays<FewsTimeSeriesHeader>(timeSeriesArrayResult.getObject0()), timeSeriesArrayResult.getObject1()));
		}));
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedForecasting(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		archiveDatabaseForecastImportRequests.forEach(archiveDatabaseImportRequest -> {
			if(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size() != archiveDatabaseImportRequest.getTaskRunIds().size())
				throw new IllegalArgumentException("archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size() != archiveDatabaseImportRequest.getTaskRunIds().size()");});

		var timeSeriesArrays = Collections.synchronizedList(new ArrayList<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest ->
			IntStream.range(0, archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().size()).parallel().forEach(i -> {
				var timeSeriesArrayResult = importSimulatedForecasting(archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().get(i), archiveDatabaseImportRequest.getTaskRunIds().get(i));
				if(timeSeriesArrayResult != null && timeSeriesArrayResult.getObject0().getHeader() != null && !timeSeriesArrayResult.getObject0().getHeader().equals(FewsTimeSeriesHeader.NONE) && !timeSeriesArrayResult.getObject0().getHeader().isNone())
					timeSeriesArrays.add(new Box<>(new TimeSeriesArrays<FewsTimeSeriesHeader>(timeSeriesArrayResult.getObject0()), timeSeriesArrayResult.getObject1()));
			})
		);
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @param taskRunId taskRunId
	 * @return Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importSimulatedForecasting(FewsTimeSeriesHeader fewsTimeSeriesHeader, String taskRunId) {
		var qualifierIds = IntStream.range(0, fewsTimeSeriesHeader.getQualifierCount()).mapToObj(fewsTimeSeriesHeader::getQualifierId).toList();
		var qualifierId = new JSONArray(qualifierIds.stream().sorted().toList()).toString();
		var moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		var locationId = fewsTimeSeriesHeader.getLocationId();
		var parameterId = fewsTimeSeriesHeader.getParameterId();
		var encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		var ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		var ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.debug(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s, taskRunId=%s",
				moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId, taskRunId));
		}
		else {
			var query = new LinkedHashMap<String, Object>(Map.of(
			"moduleInstanceId", moduleInstanceId,
			"locationId", locationId,
			"parameterId", parameterId,
			"qualifierId", qualifierId,
			"encodedTimeStepId", encodedTimeStepId,
			"ensembleId", ensembleId,
			"ensembleMemberId", ensembleMemberId,
			"taskRunId", taskRunId));

			try(var results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING), query).iterator()) {
				if (results.hasNext()) {
					var result = results.next();
					var timeSeriesHeaderResult = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_FORECASTING, result);
					return new Box<>(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeaderResult.getObject0(), (List<Map<String, Object>>)result.get("timeseries")), timeSeriesHeaderResult.getObject1());
				}
			}
		}
		return null;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importSimulatedHistorical(FewsTimeSeriesHeader fewsTimeSeriesHeader) {
		return importTimeSeriesType(fewsTimeSeriesHeader, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL);
	}

	/**
	 *
	 * @param archiveDatabaseForecastImportRequests archiveDatabaseForecastImportRequests
	 * @return List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>>
	 */
	@Override
	public List<TimeSeriesArrays<FewsTimeSeriesHeader>> importExternalForecasting(Set<ArchiveDatabaseForecastImportRequest> archiveDatabaseForecastImportRequests) {
		var timeSeriesArrays = Collections.synchronizedList(new ArrayList<TimeSeriesArrays<FewsTimeSeriesHeader>>());
		archiveDatabaseForecastImportRequests.parallelStream().forEach(archiveDatabaseImportRequest -> archiveDatabaseImportRequest.getFewsTimeSeriesHeaders().parallelStream().forEach(fewsTimeSeriesHeader -> {
			var timeSeriesArrayResult = importExternalForecasting(fewsTimeSeriesHeader);
			if(timeSeriesArrayResult != null && timeSeriesArrayResult.getObject0().getHeader() != null && !timeSeriesArrayResult.getObject0().getHeader().equals(FewsTimeSeriesHeader.NONE) && !timeSeriesArrayResult.getObject0().getHeader().isNone() && !timeSeriesArrayResult.getObject0().isEmpty())
				timeSeriesArrays.add(new TimeSeriesArrays<FewsTimeSeriesHeader>(timeSeriesArrayResult.getObject0()));
		}));
		return timeSeriesArrays;
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importExternalForecasting(FewsTimeSeriesHeader fewsTimeSeriesHeader) {
		return importTimeSeriesType(fewsTimeSeriesHeader, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, nl.wldelft.fews.system.data.timeseries.TimeSeriesType.EXTERNAL_FORECASTING);
	}

	/**
	 *
	 * @param fewsTimeSeriesHeader fewsTimeSeriesHeader
	 * @return Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>
	 */
	private Box<TimeSeriesArray<FewsTimeSeriesHeader>, SystemActivityDescriptor> importTimeSeriesType(FewsTimeSeriesHeader fewsTimeSeriesHeader, TimeSeriesType timeSeriesType, nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType) {
		var qualifierIds = IntStream.range(0, fewsTimeSeriesHeader.getQualifierCount()).mapToObj(fewsTimeSeriesHeader::getQualifierId).toList();
		var qualifierId = new JSONArray(qualifierIds.stream().sorted().toList()).toString();
		var moduleInstanceId = fewsTimeSeriesHeader.getModuleInstanceId();
		var locationId = fewsTimeSeriesHeader.getLocationId();
		var parameterId = fewsTimeSeriesHeader.getParameterId();
		var encodedTimeStepId = fewsTimeSeriesHeader.getTimeStep() == null ? null : fewsTimeSeriesHeader.getTimeStep().getEncoded();
		var ensembleId = fewsTimeSeriesHeader.getEnsembleId() == null || fewsTimeSeriesHeader.getEnsembleId().equals("none") || fewsTimeSeriesHeader.getEnsembleId().equals("main") ? "" : fewsTimeSeriesHeader.getEnsembleId();
		var ensembleMemberId = fewsTimeSeriesHeader.getEnsembleMemberId() == null || fewsTimeSeriesHeader.getEnsembleMemberId().equals("none") || fewsTimeSeriesHeader.getEnsembleMemberId().equals("0") ? "" : fewsTimeSeriesHeader.getEnsembleMemberId();
		var forecastTime = new Date(fewsTimeSeriesHeader.getForecastTime());

		if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
			logger.debug(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s, ensembleId=%s, ensembleMemberId=%s, forecastTime=%s",
				moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId, ensembleId, ensembleMemberId, forecastTime));
		}
		else {
			var query = new LinkedHashMap<String, Object>(Map.of(
			"moduleInstanceId", moduleInstanceId,
			"locationId", locationId,
			"parameterId", parameterId,
			"qualifierId", qualifierId,
			"encodedTimeStepId", encodedTimeStepId,
			"ensembleId", ensembleId,
			"ensembleMemberId", ensembleMemberId,
			"forecastTime", forecastTime));

			try(var results = new ReadSingletons().read(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType), query).iterator()) {
				if (results.hasNext()) {
					var result = results.next();
					var timeSeriesHeaderResult = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, fewsTimeSeriesType, result);
					return new Box<>(TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeaderResult.getObject0(), (List<Map<String, Object>>)result.get("timeseries")), timeSeriesHeaderResult.getObject1());
				}
			}
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
	public Set<Integer> getAvailableYears(List<FewsTimeSeriesHeader> timeSeriesHeaders, Period period) {
		if(period.getEndDate().before(period.getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL);
		var availableYears = Collections.synchronizedSet(new HashSet<Integer>());
		timeSeriesHeaders.parallelStream().forEach(timeSeriesHeader -> {

			var qualifierIds = new ArrayList<>();
			for (int j = 0; j < timeSeriesHeader.getQualifierCount(); j++)
				qualifierIds.add(timeSeriesHeader.getQualifierId(j));
			var qualifierId = new JSONArray(qualifierIds.stream().sorted().toList()).toString();
			var moduleInstanceId = timeSeriesHeader.getModuleInstanceId();
			var locationId = timeSeriesHeader.getLocationId();
			var parameterId = timeSeriesHeader.getParameterId();
			var encodedTimeStepId = timeSeriesHeader.getTimeStep() == null ? null : timeSeriesHeader.getTimeStep().getEncoded();

			if (moduleInstanceId == null || locationId == null || parameterId == null || encodedTimeStepId == null){
				logger.debug(String.format("Missing Required Query Value: moduleInstanceId=%s, locationId=%s, parameterId=%s, encodedTimeStepId=%s, qualifierId=%s",
					moduleInstanceId, locationId, parameterId, encodedTimeStepId, qualifierId));
			}
			else {
				var query = new LinkedHashMap<String, Object>(Map.of(
				"moduleInstanceId", moduleInstanceId,
				"locationId", locationId,
				"parameterId", parameterId,
				"qualifierId", qualifierId,
				"encodedTimeStepId", encodedTimeStepId));

				var years = new HashSet<Integer>();
				database.distinct(collection, "startTime", query, Date.class).forEach(s -> years.add(DateUtil.getLocalDateTime(s).getYear()));
				database.distinct(collection, "endTime", query, Date.class).forEach(s -> years.add(DateUtil.getLocalDateTime(s).getYear()));

				if(!years.isEmpty()){
					var yearQueries = new ArrayList<Map<String, Object>>();
					var startDate = DateUtil.getLocalDateTime(period.getStartDate());
					var endDate = DateUtil.getLocalDateTime(period.getEndDate());
					LongStream.rangeClosed(Collections.min(years), Collections.max(years)).forEach(year -> yearQueries.add(Map.of("and", List.of(
						Map.of("endTime", Map.of("gte", DateUtil.getDate(startDate.plusYears(year - startDate.getYear())))),
						Map.of("startTime", Map.of("lte", DateUtil.getDate(endDate.plusYears(year - startDate.getYear()))))
					))));
					query.put("or", yearQueries);

					database.aggregateAvailableYears(collection,
							query,
							Map.of("startTime", 1, "endTime", 1),
							Map.of("startTime", "startTime", "endTime", "endTime")).forEach(result -> {
						int startYear = DateUtil.getLocalDateTime((Date)result.get("startTime")).getYear();
						int endYear = DateUtil.getLocalDateTime((Date)result.get("endTime")).getYear();
						IntStream.rangeClosed(startYear, endYear).forEach(availableYears::add);
					});
				}
			}
		});
		return availableYears;
	}

	/**
	 *
	 * @param taskRunId taskRunId
	 * @param timeSeriesType timeSeriesType
	 * @return TimeSeriesArrays<FewsTimeSeriesHeader>
	 */
	@Override
	public Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor> getTimeSeriesForTaskRun(String taskRunId, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		var timeSeriesArrays = new ArrayList<TimeSeriesArray<FewsTimeSeriesHeader>>();
		var systemActivityDescriptors = new ArrayList<SystemActivityDescriptor>();

		database.find(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType)), Map.of("taskRunId", taskRunId)).forEach(result -> {
			var timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(TimeSeriesValueType.SCALAR, timeSeriesType, result);
			systemActivityDescriptors.add(timeSeriesHeader.getObject1());
			TimeSeriesArray<FewsTimeSeriesHeader> timeSeriesArray = TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), (List<Map<String, Object>>)result.get("timeseries"));
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
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		var ensembleMembers = new HashSet<String>();
		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		database.distinct(collection, "ensembleMemberId", Map.of(
		"locationId", locationId,
		"parameterId", parameterId,
		"moduleInstanceId", Map.of("in", new ArrayList<>(moduleInstanceIds)),
		"ensembleId", ensembleId,
		"qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().toList()).toString()), String.class).forEach(ensembleMemberId -> {
				if(ensembleMemberId != null && !ensembleMemberId.trim().equals("none") && !ensembleMemberId.trim().equals("0") && !ensembleMemberId.trim().isEmpty())
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
	public Set<String> getModuleInstanceIds(String locationId, String parameterId, String ensembleId, String[] qualifiers, nl.wldelft.fews.system.data.timeseries.TimeSeriesType timeSeriesType) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		var collectionKeys = nl.fews.archivedatabase.common.shared.database.Index.getKeys(collection);

		var query = new LinkedHashMap<String, Object>(Map.of("locationId", locationId, "parameterId", parameterId, "qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().toList()).toString()));
		if(collectionKeys.contains("ensembleId"))
			query.put("ensembleId", ensembleId);

		return database.distinct(collection, "moduleInstanceId", query, String.class);
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
	public List<SimulatedTaskRunInfo> getSimulatedTaskRunInfos(String locationId, String parameterId, String moduleInstanceId, String ensembleId, String[] qualifiers, String encodedTimeStepId, Period period, int forecastCount) {
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		var simulatedTaskRunInfos = new ArrayList<SimulatedTaskRunInfo>();

		database.aggregateSimulatedTaskRunInfos(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_HISTORICAL),
			Map.of("locationId", locationId, "parameterId", parameterId, "moduleInstanceId", moduleInstanceId, "ensembleId", ensembleId, "encodedTimeStepId", encodedTimeStepId, "qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().toList()).toString(), "forecastTime", Map.of("gte", period.getStartDate(), "lte", period.getEndDate())),
			Map.of("workflowId", "runInfo.workflowId", "taskRunId", "taskRunId", "forecastTime", "forecastTime", "dispatchTime", "runInfo.dispatchTime"),
			Map.of("forecastTime", -1, "taskRunId", 1, "workflowId", 1, "dispatchTime", 1),
			forecastCount).forEach(result ->
			simulatedTaskRunInfos.add(new SimulatedTaskRunInfo((String)result.get("workflowId"), (String)result.get("taskRunId"), ((Date)result.get("forecastTime")).getTime(), ((Date)result.get("dispatchTime")).getTime())));

		database.aggregateSimulatedTaskRunInfos(TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesType.SCALAR_SIMULATED_FORECASTING),
			Map.of("locationId", locationId, "parameterId", parameterId, "moduleInstanceId", moduleInstanceId, "ensembleId", ensembleId, "encodedTimeStepId", encodedTimeStepId, "qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().toList()).toString(), "forecastTime", Map.of("gte", period.getStartDate(), "lte", period.getEndDate())),
			Map.of("workflowId", "runInfo.workflowId", "taskRunId", "taskRunId", "forecastTime", "forecastTime", "dispatchTime", "runInfo.dispatchTime"),
			Map.of("forecastTime", -1, "taskRunId", 1, "workflowId", 1, "dispatchTime", 1),
			forecastCount).forEach(result ->
			simulatedTaskRunInfos.add(new SimulatedTaskRunInfo((String)result.get("workflowId"), (String)result.get("taskRunId"), ((Date)result.get("forecastTime")).getTime(), ((Date)result.get("dispatchTime")).getTime())));

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
		ensembleId = ensembleId == null || ensembleId.equals("none") || ensembleId.equals("main") ? "" : ensembleId;
		qualifiers = qualifiers == null ? new String[]{} : qualifiers;

		var longListBuilder = new LongListBuilder();

		var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, timeSeriesType));
		database.aggregateExternalForecastTimes(collection,
			Map.of("locationId", locationId, "parameterId", parameterId, "moduleInstanceId", moduleInstanceId, "ensembleId", ensembleId, "qualifierId", new JSONArray(Arrays.stream(qualifiers).sorted().toList()).toString(), "forecastTime", Map.of("gte", period.getStartDate(), "lte", period.getEndDate())),
			"forecastTime",
			Map.of("forecastTime", -1),
			forecastCount).forEach(result -> longListBuilder.add(((Date)result).getTime()));

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
