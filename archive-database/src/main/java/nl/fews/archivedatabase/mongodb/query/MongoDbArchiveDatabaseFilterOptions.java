package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseFilterOptions;
import nl.wldelft.util.timeseries.TimeStep;

import java.util.Set;

public class MongoDbArchiveDatabaseFilterOptions implements ArchiveDatabaseFilterOptions {
	@Override
	public Set<String> getParameterIds() {
		return null;
	}

	@Override
	public Set<String> getModuleInstanceIds() {
		return null;
	}

	@Override
	public Set<TimeStep> getTimeSteps() {
		return null;
	}
}
