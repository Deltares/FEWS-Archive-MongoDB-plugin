package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseFilterOptions;
import nl.wldelft.util.timeseries.TimeStep;

import java.util.Set;

/**
 *
 */
public record MongoDbArchiveDatabaseFilterOptions(Set<String> parameterIds, Set<String> moduleInstanceIds, Set<TimeStep> timeSteps) implements ArchiveDatabaseFilterOptions {

	/**
	 *
	 * @return Set<String>
	 */
	@Override
	public Set<String> getParameterIds() {
		return parameterIds;
	}

	/**
	 *
	 * @return Set<String>
	 */
	@Override
	public Set<String> getModuleInstanceIds() {
		return moduleInstanceIds;
	}

	/**
	 *
	 * @return Set<TimeStep>
	 */
	@Override
	public Set<TimeStep> getTimeSteps() {
		return timeSteps;
	}
}
