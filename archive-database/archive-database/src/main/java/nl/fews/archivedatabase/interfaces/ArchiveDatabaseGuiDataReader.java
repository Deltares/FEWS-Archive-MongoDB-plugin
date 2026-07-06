//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import java.util.Set;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseResultSearchParameters;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;

public interface ArchiveDatabaseGuiDataReader {
    Set<String> getSourceIds(TimeSeriesType timeSeriesType);

    ArchiveDatabaseReadResult read(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters);

    ArchiveDatabaseSummary getSummary(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters);

    ArchiveDatabaseFilterOptions getFilterOptions(String areaId, TimeSeriesType timeSeriesType, Set<String> sourceIds);
}
