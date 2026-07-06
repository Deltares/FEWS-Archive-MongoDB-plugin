//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.OpenArchiveToArchiveDatabaseMigrator;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;

public interface ArchiveDatabase<T extends FewsTimeSeriesHeader> {
    ArchiveDatabaseTimeSeriesExporter<T> getArchiveTimeSeriesExporter();

    OpenArchiveToArchiveDatabaseMigrator getOpenArchiveToArchiveDatabaseMigrator();

    ArchiveDatabaseTimeSeriesReader getArchiveDataBaseTimeSeriesReader();

    ArchiveDatabaseGuiDataReader getArchiveDatabaseGuiDataReader();

    void setArchiveDatabaseUrl(String url);

    void setUserNamePassword(String userName, String password);

    void setHeaderProvider(FewsTimeSeriesHeaderProvider headerProvider);
}
