package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.opendatabase.ArchiveDatabaseUnitConverter;

public class SimulatedHistorical extends ExternalForecasting implements TimeSeries {

	/**
	 *
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter. null if no conversion is to be performed.
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public SimulatedHistorical(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter, ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		super(archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
	}
}
