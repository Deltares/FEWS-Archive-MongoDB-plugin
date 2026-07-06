//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.HeaderRequest;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.Box;;

public interface FewsTimeSeriesHeaderProvider {
    Box<FewsTimeSeriesHeader, SystemActivityDescriptor> getHeader(HeaderRequest headerRequest);
}
