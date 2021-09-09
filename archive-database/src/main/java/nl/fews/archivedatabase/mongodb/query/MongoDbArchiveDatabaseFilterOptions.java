package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseFilterOptions;
import nl.wldelft.util.timeseries.TimeStep;

import java.util.Set;

/**
 *
 */
public final class MongoDbArchiveDatabaseFilterOptions implements ArchiveDatabaseFilterOptions {

	/**
	 *
	 */
	private final Set<String> parameterIds;

	/**
	 *
	 */
	private final Set<String> moduleInstanceIds;

	/**
	 *
	 */
	private final Set<TimeStep> timeSteps;

	/**
	 *
	 * @param parameterIds parameterIds
	 * @param moduleInstanceIds moduleInstanceIds
	 * @param timeSteps timeSteps
	 */
	public MongoDbArchiveDatabaseFilterOptions(Set<String> parameterIds, Set<String> moduleInstanceIds, Set<TimeStep> timeSteps) {
		this.parameterIds = parameterIds;
		this.moduleInstanceIds = moduleInstanceIds;
		this.timeSteps = timeSteps;
	}

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
