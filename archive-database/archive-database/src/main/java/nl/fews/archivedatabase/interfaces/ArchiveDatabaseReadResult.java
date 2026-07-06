//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import java.util.Iterator;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.Box;
import nl.wldelft.util.timeseries.TimeSeriesArrays;

public interface ArchiveDatabaseReadResult extends Iterator<Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor>> {
    Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor> next();

    boolean hasNext();
}
