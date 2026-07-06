//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.SimulatedExportConfigSettings;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;

public interface ArchiveDatabaseTimeSeriesExporter<T extends TimeSeriesHeader> extends ArchiveDatabaseFewsEnvironmentSettings {
    void insertExternalHistoricalTimeSeries(TimeSeriesArrays<T> timeSeriesArrays, String areaId, String sourceId);

    void insertSimulatedForecastingTimeSeries(TimeSeriesArrays<T> timeSeriesArrays, String areaId, String sourceId);

    void insertSimulatedHistoricalTimeSeries(TimeSeriesArrays<T> timeSeriesArrays, String areaId, String sourceId);

    void insertExternalForecastingTimeSeries(TimeSeriesArrays<T> timeSeriesArrays, String areaId, String sourceId);

    void setSimulatedExportSettings(SimulatedExportConfigSettings simulatedExportConfigSettings);
}
