package nl.fews.archivedatabase.mongodb.export.timeseries;

import nl.fews.archivedatabase.mongodb.export.interfaces.TimeSeries;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;

public class SimulatedForecasting extends ExternalForecasting implements TimeSeries {

	/**
	 *
	 * @param archiveDatabaseUnitConverter archiveDatabaseUnitConverter. null if no conversion is to be performed.
	 * @param archiveDatabaseTimeConverter archiveDatabaseTimeConverter. null if no conversion is to be performed.
	 */
	public SimulatedForecasting(ArchiveDatabaseUnitConverter archiveDatabaseUnitConverter, ArchiveDatabaseTimeConverter archiveDatabaseTimeConverter){
		super(archiveDatabaseUnitConverter, archiveDatabaseTimeConverter);
	}
}
