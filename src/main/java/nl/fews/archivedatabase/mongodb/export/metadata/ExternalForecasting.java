package nl.fews.archivedatabase.mongodb.export.metadata;

import nl.fews.archivedatabase.mongodb.export.interfaces.MetaData;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
public class ExternalForecasting extends ExternalHistorical implements MetaData {

	/**
	 *
	 * @param areaId areaId
	 * @param sourceId sourceId
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter. null if no conversion is to be performed.
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public ExternalForecasting(String areaId, String sourceId, ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter, ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		super(areaId, sourceId, archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
	}

	/**
	 * @param header FEWS timeseries header
	 * @return bson document representing the meta data of this timeseries
	 */
	@Override
	public Document getMetaData(TimeSeriesHeader header){
		Document document = super.getMetaData(header);

		int ensembleMemberIndex = header.getEnsembleMemberIndex();
		document.append("ensembleMemberIndex", ensembleMemberIndex);

		return document;
	}
}
