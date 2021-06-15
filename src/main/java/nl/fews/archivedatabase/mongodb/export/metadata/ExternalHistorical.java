package nl.fews.archivedatabase.mongodb.export.metadata;

import nl.fews.archivedatabase.mongodb.export.interfaces.MetaData;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;

import java.util.Date;

public class ExternalHistorical implements MetaData {

	private final String sourceId;
	private final String areaId;
	private final ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter;
	private final ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter;

	/**
	 *
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter. null if no conversion is to be performed.
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public ExternalHistorical(String areaId, String sourceId, ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter, ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		this.sourceId = sourceId == null ? "" : sourceId;
		this.areaId = areaId == null ? "" : areaId;
		this.archiveDatabaseUnitConverter = archiveDatabaseUnitConverter;
		this.archiveDatabaseTimeConverter = archiveDatabaseTimeConverter;
	}

	/**
	 *
	 * @param header FEWS timeseries header
	 * @return bson document representing the meta data of this timeseries
	 */
	public Document getMetaData(TimeSeriesHeader header){
		Document document = new Document();

		int min = (int)(header.getTimeStep().getMinimumStepMillis() / 1000 / 60);
		int max = (int)(header.getTimeStep().getMaximumStepMillis() / 1000 / 60);
		Date now = new Date();
		int timeStepMinutes = min == max ? min : (min / 60) % 2 == 0 ? min : max;
		String unit = header.getUnit() == null ? "" : header.getUnit();
		String locationName = header.getLocationName() == null ? "" : header.getLocationName();
		String parameterName = header.getParameterName() == null ? "" : header.getParameterName();
		String parameterType = header.getParameterType() == null || header.getParameterType().getName() == null ? "" : header.getParameterType().getName();
		String timeStepLabel = header.getTimeStep() == null || header.getTimeStep().getLabel() == null ? "" : header.getTimeStep().getLabel();

		document.append("sourceId", sourceId);
		document.append("areaId", areaId);
		document.append("unit", unit);
		if(archiveDatabaseUnitConverter != null) document.append("displayUnit", archiveDatabaseUnitConverter.getOutputUnitType(header.getParameterId()));
		document.append("locationName", locationName);
		document.append("parameterName", parameterName);
		document.append("parameterType", parameterType);
		document.append("timeStepLabel", timeStepLabel);
		document.append("timeStepMinutes", timeStepMinutes);
		if(archiveDatabaseTimeConverter != null) document.append("localTimeZone", archiveDatabaseTimeConverter.getLocalTimeZone().getID());
		document.append("archiveTime", now);
		document.append("modifiedTime", now);

		return document;
	}
}
