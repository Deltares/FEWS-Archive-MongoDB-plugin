package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.fews.archivedatabase.mongodb.export.utils.DateUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class ExternalHistorical implements TimeSeries {

	private final ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter;
	private final ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter;

	/**
	 *
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter. null if no conversion is to be performed.
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public ExternalHistorical(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter, ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		this.archiveDatabaseUnitConverter = archiveDatabaseUnitConverter;
		this.archiveDatabaseTimeConverter = archiveDatabaseTimeConverter;
	}

	/**
	 *
	 * @param timeSeriesArray FEWS timeseries array
	 * @return the sorted list of timeseries event documents
	 */
	public List<Document> getTimeSeries(TimeSeriesArray<TimeSeriesHeader> timeSeriesArray){
		List<Document> timeseriesDocuments = new ArrayList<>();
		Date[] localTimes = archiveDatabaseTimeConverter == null ? null : DateUtil.getDates(archiveDatabaseTimeConverter.convert(timeSeriesArray.toTimesArray()));
		float[] displayValues = archiveDatabaseUnitConverter == null ? null :archiveDatabaseUnitConverter.convert(timeSeriesArray.getHeader().getParameterId(), timeSeriesArray.toFloatArray());
		for (int k = 0; k < timeSeriesArray.size(); k++) {
			Object value = Float.isNaN(timeSeriesArray.getValue(k)) ? null : timeSeriesArray.getValue(k);
			Object displayValue = value == null || displayValues == null ? null : displayValues[k];

			Document timeseriesDocument = new Document();
			timeseriesDocument.append("t", new Date(timeSeriesArray.getTime(k)));
			if(localTimes != null) timeseriesDocument.append("lt", localTimes[k]);
			timeseriesDocument.append("v", value);
			if(displayValue != null) timeseriesDocument.append("dv", displayValue);
			timeseriesDocument.append("f", timeSeriesArray.getFlag(k));
			if(timeSeriesArray.getComment(k) != null) timeseriesDocument.append("c", timeSeriesArray.getComment(k));
			timeseriesDocuments.add(timeseriesDocument);
		}
		timeseriesDocuments.sort(Comparator.comparing(c -> c.getDate("t")));
		return timeseriesDocuments;
	}
}
