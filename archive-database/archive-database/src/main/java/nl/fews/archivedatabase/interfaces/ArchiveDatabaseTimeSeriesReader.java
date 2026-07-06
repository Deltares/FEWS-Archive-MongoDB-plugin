//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import java.util.List;
import java.util.Set;
import nl.wldelft.fews.system.data.externaldatasource.importrequestbuilder.SimulatedTaskRunInfo;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.LongUnmodifiableList;
import nl.wldelft.util.Period;
import nl.wldelft.util.timeseries.TimeSeriesArrays;

public interface ArchiveDatabaseTimeSeriesReader {
    List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedForecasting(Set<ArchiveDatabaseForecastImportRequest> singleExternalDataImportRequest);

    List<TimeSeriesArrays<FewsTimeSeriesHeader>> importExternalForecasting(Set<ArchiveDatabaseForecastImportRequest> singleExternalDataImportRequest);

    List<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>> importSimulatedHistorical(Set<ArchiveDatabaseForecastImportRequest> singleExternalDataImportRequest, boolean stitched);

    Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor> getTimeSeriesForTaskRun(String taskRunId, TimeSeriesType timeSeriesType);

    TimeSeriesArrays<FewsTimeSeriesHeader> importExternalHistorical(Period observedPeriod, TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays);

    TimeSeriesArrays<FewsTimeSeriesHeader> importStitchedSimulatedHistorical(Period observedPeriod, TimeSeriesArrays<FewsTimeSeriesHeader> timeSeriesArrays);

    Set<Integer> getAvailableYears(List<FewsTimeSeriesHeader> timeSeriesHeaders, Period period);

    Set<String> getEnsembleMembers(String locationId, String parameterId, Set<String> moduleInstanceId, String ensembleId, String[] qualifiers, TimeSeriesType timeSeriesType);

    Set<String> getModuleInstanceIds(String locationId, String parameterId, String ensembleId, String[] qualifiers, TimeSeriesType timeSeriesType);

    List<SimulatedTaskRunInfo> getSimulatedTaskRunInfos(String locationId, String parameterId, String moduleInstanceId, String ensembleId, String[] qualifiers, String timeStep, Period period, int forecastCount);

    LongUnmodifiableList searchForExternalForecastTimes(String locationId, String parameterId, String moduleInstanceId, String ensembleId, String[] qualifiers, TimeSeriesType timeSeriesType, Period period, int forecastCount);
}
