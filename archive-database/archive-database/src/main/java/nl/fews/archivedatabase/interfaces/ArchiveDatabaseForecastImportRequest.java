//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import nl.wldelft.fews.system.data.requestimporter.SingleExternalDataImportRequest;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeaders;
import nl.wldelft.util.Arguments;
import nl.wldelft.util.Period;

public final class ArchiveDatabaseForecastImportRequest implements SingleExternalDataImportRequest {
    private final List<FewsTimeSeriesHeader> timeSeriesHeaders;
    private final List<String> taskRunIds;

    public ArchiveDatabaseForecastImportRequest(List<FewsTimeSeriesHeader> timeSeriesHeaders, List<String> taskRunIds) {
        this.timeSeriesHeaders = Collections.unmodifiableList(timeSeriesHeaders);
        this.taskRunIds = Collections.unmodifiableList(taskRunIds);
    }

    public ArchiveDatabaseForecastImportRequest(FewsTimeSeriesHeaders fewsTimeSeriesHeaders) {
        Arguments.require.notNull(fewsTimeSeriesHeaders);
        ArrayList var2 = new ArrayList();
        Objects.requireNonNull(var2);
        fewsTimeSeriesHeaders.forEach(var2::add);
        ArrayList var3 = new ArrayList();
        fewsTimeSeriesHeaders.forEach((var1) -> var3.add(var1.getTaskRunDescriptor().getId()));
        this.timeSeriesHeaders = var2;
        this.taskRunIds = var3;
    }

    public Period period() {
        return null;
    }

    public List<FewsTimeSeriesHeader> getFewsTimeSeriesHeaders() {
        return Collections.unmodifiableList(this.timeSeriesHeaders);
    }

    public List<String> getTaskRunIds() {
        return Collections.unmodifiableList(this.taskRunIds);
    }
}
