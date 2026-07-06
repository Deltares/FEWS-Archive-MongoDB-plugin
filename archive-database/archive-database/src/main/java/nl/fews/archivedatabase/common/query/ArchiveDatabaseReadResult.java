package nl.fews.archivedatabase.common.query;

import nl.fews.archivedatabase.common.shared.utils.TimeSeriesArrayUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.runs.SystemActivityDescriptor;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.Box;
import nl.wldelft.util.timeseries.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
@SuppressWarnings("unchecked")
public class ArchiveDatabaseReadResult implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseReadResult {

	/**
	 *
	 */
	private final Iterator<Map<String, Object>> result;

	/**
	 *
	 */
	private final TimeSeriesType timeSeriesType;

	/**
	 *
	 */
	private final TimeSeriesValueType timeSeriesValueType;

	/**
	 *
	 * @param result result
	 * @param timeSeriesValueType valueType
	 * @param timeSeriesType timeSeriesType
	 */
	public ArchiveDatabaseReadResult(Iterator<Map<String, Object>> result, TimeSeriesValueType timeSeriesValueType, TimeSeriesType timeSeriesType) {
		this.result = result;
		this.timeSeriesType = timeSeriesType;
		this.timeSeriesValueType = timeSeriesValueType;
	}

	/**
	 *
	 * @return TimeSeriesArrays<FewsTimeSeriesHeader>
	 */
	@Override
	public Box<TimeSeriesArrays<FewsTimeSeriesHeader>, SystemActivityDescriptor> next() {
		var next = result.next();
		var timeSeriesHeader = TimeSeriesArrayUtil.getTimeSeriesHeader(timeSeriesValueType, timeSeriesType, next);
		var timeSeriesArray = TimeSeriesArrayUtil.getTimeSeriesArray(timeSeriesHeader.getObject0(), (List<Map<String, Object>>)next.get("timeseries"));
		return new Box<>(new TimeSeriesArrays<>(timeSeriesArray), timeSeriesHeader.getObject1());
	}

	/**
	 *
	 * @return boolean
	 */
	@Override
	public boolean hasNext() {
		return result.hasNext();
	}
}
