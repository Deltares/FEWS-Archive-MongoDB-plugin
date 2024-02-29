package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseSummary;

/**
 *
 */
public class MongoDbArchiveDatabaseSummary implements ArchiveDatabaseSummary {

	/**
	 *
	 */
	private final int numberOfParameters;

	/**
	 *
	 */
	private final int numberOfModuleInstanceIds;

	/**
	 *
	 */
	private final int numberOfTimeSeries;

	/**
	 *
	 * @param numberOfParameters numberOfParameters
	 * @param numberOfModuleInstanceIds numberOfModuleInstanceIds
	 * @param numberOfTimeSeries numberOfTimeSeries
	 */
	public MongoDbArchiveDatabaseSummary(int numberOfParameters, int numberOfModuleInstanceIds, int numberOfTimeSeries) {
		this.numberOfParameters = numberOfParameters;
		this.numberOfModuleInstanceIds = numberOfModuleInstanceIds;
		this.numberOfTimeSeries = numberOfTimeSeries;
	}

	/**
	 *
	 * @return int
	 */
	@Override
	public int numberOfParameters() {
		return numberOfParameters;
	}

	/**
	 *
	 * @return int
	 */
	@Override
	public int numberOfModuleInstanceIds() {
		return numberOfModuleInstanceIds;
	}

	/**
	 *
	 * @return int
	 */
	@Override
	public int numberOfTimeSeries() {
		return numberOfTimeSeries;
	}
}
