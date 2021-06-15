package nl.fews.archivedatabase.mongodb.export;

import nl.fews.archivedatabase.mongodb.export.enums.TimeSeriesType;
import nl.fews.archivedatabase.mongodb.export.interfaces.*;
import nl.fews.archivedatabase.mongodb.export.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.Properties;
import nl.wldelft.util.timeseries.*;

import org.bson.Document;

import java.util.*;

/**
 * Iterates through a series of timeseries arrays converting them to bson documents for mongodb consumption.
 * Inserts resulting documents if no document is found at the collection key coordinates
 * Otherwise removed, updated, or replaced, depending on the extent of changes.  The _id key and durable key is preserved
 * Documents are binned (bucketed) according to UTC event date, either yearly or monthly according to timeStepMinutes
 * null values are converted to empty string except for event values, where if NaN or null => null
 * event dates are are assumed to be non-null and unique for each timeSeriesArray collection
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
 *   lt = local time
 *   dv = display value
 *  All times are in UTC except where prefixed by 'local'
 *  local time values will not be unique in areas where DST is used.
 *  to get a unique representation of local times, derive the hour offset from UTC time.
 *  timeseries stored in mongo db are guaranteed to be in ascending order and unique
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MongoDbArchiveDatabasePlugin implements ArchiveDatabase {

    //region Variable Declaration

    /**
     * The key-value pair properties passed to this implementation
     * e.g. localTimeZone
     */
    private Properties properties;

    /**
     * THe unit converter
     */
    private ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter;

    /**
     * The time converter
     */
    private ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter;

    /**
     * The base url format string template for connecting to a mongo db instance
     * e.g. mongodb://mongo.infisys.net:27018/FEWS_ARCHIVE_TEST?authSource=admin&tls=true
     * e.g. mongodb://[server|dns|ip]:[port]/[database]?connectionSettings
     */
    private String archiveDatabaseUrl;

    /**
     * The username to apply to the archiveDatabaseUrl
     */
    private String archiveDatabaseUserName;

    /**
     * The password to apply to the archiveDatabaseUrl
     */
    private String archiveDatabasePassword;

    /**
     *
     */
    private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.mongodb.export";

    //endregion

    //region Constructors

    /**
     * Creates a new instance of this interface implementation
     */
    public static MongoDbArchiveDatabasePlugin create() {
        return new MongoDbArchiveDatabasePlugin();
    }

    /**
     * block direct instantiation; use static create() method
     */
    private MongoDbArchiveDatabasePlugin(){

    }

    //endregion

    //region Interface Implementation

    /**
     *
     * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter
     */
    @Override
    public void setUnitConverter(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter) {
        this.archiveDatabaseUnitConverter = archiveDatabaseUnitConverter;
    }

    @Override
    public void setConfigRevision(String s) {

    }

    @Override
    public void setSimulatedExportSettings(SimulatedExportConfigSettings simulatedExportConfigSettings) {

    }

    @Override
    public void executeActivity(String s, String s1) {

    }

    /**
     *
     * @param  archiveDatabaseTimeConverter archiveDatabaseTimeConverter
     */
    @Override
    public void setTimeConverter(ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter) {
        this.archiveDatabaseTimeConverter = archiveDatabaseTimeConverter;
    }

    @Override
    public void setRegionConfigInfoProvider(ArchiveDatabaseRegionConfigInfoProvider archiveDatabaseRegionConfigInfoProvider) {

    }

    /**
     * The key-value pair properties passed to this implementation
     * @param properties properties
     */
    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * The base url format string template for connecting to a mongo db instance
     * @param archiveDatabaseUrl mongodb://%s:%s@mongo.infisys.net:27018/admin?tls=true => mongodb://username:password@[server|dns|ip]:port/authDB?connectionSettings
     */
    @Override
    public void setArchiveDatabaseUrl(String archiveDatabaseUrl) {
        this.archiveDatabaseUrl = archiveDatabaseUrl;
    }

    /**
     * The user / pass to use for mongo db connections
     * @param archiveDatabaseUserName The password to apply to the archiveDatabaseUrl
     * @param archiveDatabasePassword The username to apply to the archiveDatabaseUrl
     */
    @Override
    public void setUserNamePassword(String archiveDatabaseUserName, String archiveDatabasePassword) {
        this.archiveDatabaseUserName = archiveDatabaseUserName;
        this.archiveDatabasePassword = archiveDatabasePassword;
    }

    /**
     * SEE CLASS DOCUMENTATION
     * @param timeSeriesArrays timeSeriesArrays
     * @param areaId           areaId
     * @param sourceId         sourceId
     */
    @Override
    public void insertExternalHistoricalTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
        insertTimeSeries(timeSeriesArrays, TimeSeriesType.EXTERNAL_HISTORICAL, areaId, sourceId);
    }

    /**
     * SEE CLASS DOCUMENTATION
     * @param timeSeriesArrays timeSeriesArrays
     * @param areaId           areaId
     * @param sourceId         sourceId
     */

    @Override
    public void insertExternalForecastingTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
        insertTimeSeries(timeSeriesArrays, TimeSeriesType.EXTERNAL_FORECASTING, areaId, sourceId);
    }

    /**
     * SEE CLASS DOCUMENTATION
     * @param timeSeriesArrays timeSeriesArrays
     * @param areaId           areaId
     * @param sourceId         sourceId
     */
    @Override
    public void insertSimulatedForecastingTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
        insertTimeSeries(timeSeriesArrays, TimeSeriesType.SIMULATED_FORECASTING, areaId, sourceId);
    }

    /**
     * SEE CLASS DOCUMENTATION
     * @param timeSeriesArrays timeSeriesArrays
     * @param areaId           areaId
     * @param sourceId         sourceId
     */
    @Override
    public void insertSimulatedHistoricalTimeSeries(TimeSeriesArrays timeSeriesArrays, String areaId, String sourceId) {
        insertTimeSeries(timeSeriesArrays, TimeSeriesType.SIMULATED_HISTORICAL, areaId, sourceId);
    }

    //endregion

    //region Private Methods

    /**
     *
     * @param timeSeriesArrays timeSeriesArrays
     * @param timeSeriesType timeSeriesType
     * @param areaId areaId
     * @param sourceId sourceId
     */
    private void insertTimeSeries(TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays, TimeSeriesType timeSeriesType, String areaId, String sourceId) {
        try{
            TimeSeries timeSeries = (TimeSeries)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "timeseries", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).
                    getConstructor(ArchiveDatabaseUnitConverter.class, ArchiveDatabaseTimeConverter.class).
                    newInstance(archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);

            MetaData metaData = (MetaData)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "metadata", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).
                    getConstructor(String.class, String.class, ArchiveDatabaseUnitConverter.class, ArchiveDatabaseTimeConverter.class).
                    newInstance(areaId, sourceId, archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);

            RunInfo runInfo = (RunInfo)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "runinfo", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).
                    getConstructor().
                    newInstance();

            Root root = (Root)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "root", TimeSeriesTypeUtil.getTimeSeriesTypeClassName(timeSeriesType))).
                    getConstructor(ArchiveDatabaseTimeConverter.class).
                    newInstance(archiveDatabaseTimeConverter);

            List<Document> ts = new ArrayList<>();

            for(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray: timeSeriesArrays.toArray()){
                TimeSeriesHeader header = timeSeriesArray.getHeader();

                List<Document> timeseriesDocuments = timeSeries.getTimeSeries(timeSeriesArray);
                Document metaDataDocument = metaData.getMetaData(header);
                Document runInfoDocument = runInfo.getRunInfo();
                Document rootDocument = root.getRoot(header, timeseriesDocuments, runInfoDocument);

                if(!metaDataDocument.isEmpty()) rootDocument.append("metaData", metaDataDocument);
                if(!runInfoDocument.isEmpty()) rootDocument.append("runInfo", runInfoDocument);
                if(!timeseriesDocuments.isEmpty()) rootDocument.append("timeseries", timeseriesDocuments);

                if(!timeseriesDocuments.isEmpty()){
                    ts.add(rootDocument);
                }
            }
            String connectionString = archiveDatabaseUserName==null || archiveDatabaseUserName.equals("") || archiveDatabasePassword==null || archiveDatabaseUrl.contains("@") ?
                    archiveDatabaseUrl :
                    archiveDatabaseUrl.replace("mongodb://", String.format("mongodb://%s:%s@", archiveDatabaseUserName, archiveDatabasePassword));

            Synchronize synchronize = (Synchronize)Class.forName(String.format("%s.MongoDbSynchronize%ss", BASE_NAMESPACE, TimeSeriesTypeUtil.getTimeSeriesTypeSyncType(timeSeriesType))).
                    getConstructor(String.class).
                    newInstance(connectionString);

            synchronize.synchronize(ts, timeSeriesType);
        }
        catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    //endregion
}
