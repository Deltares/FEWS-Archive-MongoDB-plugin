package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseSummary;

public class MongoDbArchiveDatabaseSummary implements ArchiveDatabaseSummary {
	@Override
	public int numberOfParameters() {
		return 0;
	}

	@Override
	public int numberOfModuleInstanceIds() {
		return 0;
	}

	@Override
	public int numberOfTimeSeries() {
		return 0;
	}
}
