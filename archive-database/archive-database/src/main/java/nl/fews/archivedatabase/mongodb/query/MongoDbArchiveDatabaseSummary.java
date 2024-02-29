package nl.fews.archivedatabase.mongodb.query;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseSummary;

/**
 * @param numberOfParameters
 * @param numberOfModuleInstanceIds
 * @param numberOfTimeSeries
 */
public record MongoDbArchiveDatabaseSummary(int numberOfParameters, int numberOfModuleInstanceIds, int numberOfTimeSeries) implements ArchiveDatabaseSummary {}
