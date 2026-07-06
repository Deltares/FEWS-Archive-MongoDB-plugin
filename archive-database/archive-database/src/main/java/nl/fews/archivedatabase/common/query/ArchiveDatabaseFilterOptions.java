package nl.fews.archivedatabase.common.query;

import nl.wldelft.util.timeseries.TimeStep;

import java.util.Set;

/**
 *
 */
public record ArchiveDatabaseFilterOptions(Set<String> parameterIds, Set<String> moduleInstanceIds, Set<TimeStep> timeSteps) implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseFilterOptions {

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
