package nl.fews.archivedatabase.common.export;

import nl.fews.archivedatabase.common.export.interfaces.Synchronize;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.interfaces.TimeSeries;
import nl.fews.archivedatabase.common.shared.settings.Settings;
import nl.fews.archivedatabase.common.shared.utils.BucketUtil;
import nl.fews.archivedatabase.common.shared.utils.LogUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.Properties;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Iterates through a series of timeseries arrays converting them to map documents for db consumption.
 * Inserts resulting documents if no document is found at the collection key coordinates
 * Otherwise removed, updated, or replaced, depending on the extent of changes.  The _id key and durable key is preserved
 * Documents are binned (bucketed) according to UTC event date, either yearly or monthly according to timeStepMinutes
 * null values are converted to empty string except for event values, where if NaN or null => null
 * event dates are assumed to be non-null and unique for each timeSeriesArray collection
 * The following fields are omitted if there is no local timezone specified (use null or omit to specify no timezone):
 *  - metaData.localTimeZone
 *  - timeseries.lt
 *  - localStartTime
 *  - localEndTime
 * The following fields are omitted if no unit conversion is present in RegionConfigurations/UnitConversions FEWS system relational data tables:
 *  - metaData.displayUnit
 *  - timeseries.dv
 *  The following field is omitted if its value is null:
 *  - timeseries.c
 *  timeseries value abbreviations:
 *   t = event time in UTC
 *   v = value in SI units
 *   f = flag
 *   c = comment
 *   lt = event time in local timezone
 *   dv = display value
 *   fs = flag source
 *   u = user
 *  All times are in UTC except where prefixed by 'local'
 *  local time values will not be unique in areas where DST is used.
 *  to get a unique representation of local times, derive the hour offset from UTC time.
 *  timeseries stored in db are guaranteed to be in ascending order and unique
 */
@SuppressWarnings({"unchecked"})
public class ArchiveDatabaseTimeSeriesExporter implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseTimeSeriesExporter<FewsTimeSeriesHeader> {
	/**
	 *
	 */
	private static final Logger logger = LogManager.getLogger(ArchiveDatabaseTimeSeriesExporter.class);

	/**
	 *
	 */
	private static ArchiveDatabaseTimeSeriesExporter archiveDatabaseTimeSeriesExporter = null;

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.common";

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static ArchiveDatabaseTimeSeriesExporter create() {
		if(archiveDatabaseTimeSeriesExporter == null)
			archiveDatabaseTimeSeriesExporter = new ArchiveDatabaseTimeSeriesExporter();
		return archiveDatabaseTimeSeriesExporter;
	}

	/**
	 * block direct instantiation; use static create() method
	 */
	private ArchiveDatabaseTimeSeriesExporter(){}

	/**
	 *
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter
	 */
	@Override
	public void setUnitConverter(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter) {
		Settings.put("archiveDatabaseUnitConverter", archiveDatabaseUnitConverter);
	}

	/**
	 *
	 * @param  archiveDatabaseTimeConverter archiveDatabaseTimeConverter
	 */
	@Override
	public void setTimeConverter(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter) {
		Settings.put("archiveDatabaseTimeConverter", archiveDatabaseTimeConverter);
	}

	/**
	 *
	 * @param archiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider
	 */
	@Override
	public void setRegionConfigInfoProvider(ArchiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider){
		Settings.put("archiveDatabaseRegionConfigInfoProvider", archiveDatabaseRegionConfigInfoProvider);
	}

	/**
	 * The key-value pair properties passed to this implementation
	 * @param properties properties
	 */
	@Override
	public void setProperties(Properties properties) {
		Settings.put("properties", properties);
	}

	/**
	 *
	 * @param configRevision configRevision
	 */
	@Override
	public void setConfigRevision(String configRevision) {
		Settings.put("configRevision", configRevision);
	}

	/**
	 *
	 * @param simulatedExportConfigSettings simulatedExportConfigSettings
	 */
	@Override
	public void setSimulatedExportSettings(SimulatedExportConfigSettings simulatedExportConfigSettings) {
		Settings.put("simulatedExportConfigSettings", simulatedExportConfigSettings);
	}

	/**
	 * SEE CLASS DOCUMENTATION
	 * @param timeSeriesArrays timeSeriesArrays
	 * @param areaId           areaId
	 * @param sourceId         sourceId
	 */
	@Override
	public void insertExternalHistoricalTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
		insertTimeSeries(timeSeriesArrays, TimeSeriesType.SCALAR_EXTERNAL_HISTORICAL, areaId, sourceId);
	}

	/**
	 * SEE CLASS DOCUMENTATION
	 * @param timeSeriesArrays timeSeriesArrays
	 * @param areaId           areaId
	 * @param sourceId         sourceId
	 */

	@Override
	public void insertExternalForecastingTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
		insertTimeSeries(timeSeriesArrays, TimeSeriesType.SCALAR_EXTERNAL_FORECASTING, areaId, sourceId);
	}

	/**
	 * SEE CLASS DOCUMENTATION
	 * @param timeSeriesArrays timeSeriesArrays
	 * @param areaId           areaId
	 * @param sourceId         sourceId
	 */
	@Override
	public void insertSimulatedForecastingTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
		var overwrite = Settings.get("simulatedExportConfigSettings") != null && Settings.get("simulatedExportConfigSettings", SimulatedExportConfigSettings.class).isOverwritePreviousForecast();
		insertTimeSeries(timeSeriesArrays, overwrite ? TimeSeriesType.SCALAR_SIMULATED_FORECASTING_OVERWRITE : TimeSeriesType.SCALAR_SIMULATED_FORECASTING, areaId, sourceId);
	}

	/**
	 * SEE CLASS DOCUMENTATION
	 * @param timeSeriesArrays timeSeriesArrays
	 * @param areaId           areaId
	 * @param sourceId         sourceId
	 */
	@Override
	public void insertSimulatedHistoricalTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
		var overwrite = Settings.get("simulatedExportConfigSettings") != null && Settings.get("simulatedExportConfigSettings", SimulatedExportConfigSettings.class).isOverwritePreviousForecast();
		insertTimeSeries(timeSeriesArrays, overwrite ? TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_OVERWRITE : TimeSeriesType.SCALAR_SIMULATED_HISTORICAL, areaId, sourceId);
		insertTimeSeries(timeSeriesArrays, TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED, areaId, sourceId);
	}

	/**
	 *
	 * @param timeSeriesArrays timeSeriesArrays
	 * @param timeSeriesType timeSeriesType
	 * @param areaId areaId
	 * @param sourceId sourceId
	 */
	private void insertTimeSeries(TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays, TimeSeriesType timeSeriesType, String areaId, String sourceId) {
		try{
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			var timeSeries = (TimeSeries)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "shared.timeseries", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).getConstructor().newInstance();
			var synchronize = (Synchronize)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "export.operations", String.format("Synchronize%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();

			var groupedTimeSeriesArrays = Arrays.stream(timeSeriesArrays.toArray()).collect(Collectors.groupingBy(g -> Index.getKeyDocument(BucketUtil.getBucketKeyFields(collection), timeSeries.getRoot(g.getHeader(), List.of(), timeSeries.getRunInfo(g.getHeader())))));
			var uniqueTimeSeriesArrays = new ArrayList<TimeSeriesArray<FewsTimeSeriesHeader>>();
			groupedTimeSeriesArrays.forEach((key, value) -> {
				uniqueTimeSeriesArrays.add(value.get(0));
				if (value.size() > 1) {
					var ex = new Exception("Duplicate time series array found and removed");
					logger.warn(new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("duplicatesRemoved", value.size() - 1, "duplicateValue", key))).toString(), ex);
					logger.warn("Config.Warn: {}", new JSONObject(LogUtil.getLogMessageJson(ex, Map.of("duplicatesRemoved", value.size() - 1, "duplicateValue", key))).toString(), ex);
				}
			});
			uniqueTimeSeriesArrays.parallelStream().forEach(timeSeriesArray -> {
				FewsTimeSeriesHeader header = timeSeriesArray.getHeader();

				var metaDataDocument = timeSeries.getMetaData(header, areaId, sourceId);
				var timeseriesDocuments = TimeSeriesUtil.trimNullValues(timeSeries.getEvents(timeSeriesArray, metaDataDocument));
				var runInfoDocument = timeSeries.getRunInfo(header);
				var rootDocument = timeSeries.getRoot(header, timeseriesDocuments, runInfoDocument);

				if (!metaDataDocument.isEmpty()) rootDocument.put("metaData", metaDataDocument);
				if (!runInfoDocument.isEmpty()) rootDocument.put("runInfo", runInfoDocument);
				if (!timeseriesDocuments.isEmpty()) rootDocument.put("timeseries", timeseriesDocuments);

				if (!timeseriesDocuments.isEmpty()) {
					synchronize.synchronize(rootDocument, timeSeriesType);
				}
			});
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
