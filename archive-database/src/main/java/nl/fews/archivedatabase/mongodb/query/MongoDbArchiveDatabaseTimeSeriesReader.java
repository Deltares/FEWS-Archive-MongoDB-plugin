package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.shared.database.Database;
import nl.wldelft.fews.castor.archive.types.ArchiveTimeSeriesType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.Period;

import java.util.Set;

public class MongoDbArchiveDatabaseTimeSeriesReader implements ArchiveDatabaseTimeSeriesReader {

	/**
	 *
	 */
	private static MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = null;

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static MongoDbArchiveDatabaseTimeSeriesReader create() {
		if(mongoDbArchiveDatabaseTimeSeriesReader == null)
			mongoDbArchiveDatabaseTimeSeriesReader = new MongoDbArchiveDatabaseTimeSeriesReader();
		return mongoDbArchiveDatabaseTimeSeriesReader;
	}

	@Override
	public ArchiveDatabaseReadResult read(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		return null;
	}

	@Override
	public ArchiveDatabaseSummary getSummary(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		return null;
	}

	@Override
	public ArchiveDatabaseFilterOptions getFilterOptions(String areaId, ArchiveTimeSeriesType archiveTimeSeriesType, Period period, Set<String> sourceIds) {

		switch (archiveTimeSeriesType.getType()){
			case ArchiveTimeSeriesType.OBSERVED_TYPE:
				break;
			case ArchiveTimeSeriesType.SIMULATED_TYPE:
				break;
			case ArchiveTimeSeriesType.EXTERNALFORECAST_TYPE:
				break;
			default:
				break;
		}
//		Database.aggregate()
//		db.ExternalHistoricalScalarTimeSeries.aggregate([
//				{"$sort": {"parameterId": 1}},
//		{"$group": {"_id": "$parameterId"}}]).forEach(function(r){
//			db.ExternalForecastingScalarTimeSeries.aggregate([
//					{"$match": {
//				"forecastTime": {"$gte": ISODate("2000-01-01 00:00:00.000Z"), "$lte": ISODate("2021-08-03 23:00:00.000Z")},
//				"parameterId": r._id}},
//			{"$limit": 1},
//			{"$project": {"_id": 0, "parameterId": 1}}
//        ]).forEach(function(x){
//				print(x);
//			})
//		});
		return null;
	}
}
