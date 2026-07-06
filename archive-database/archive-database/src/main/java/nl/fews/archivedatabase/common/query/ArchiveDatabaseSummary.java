package nl.fews.archivedatabase.common.query;

/**
 * @param numberOfParameters
 * @param numberOfModuleInstanceIds
 * @param numberOfTimeSeries
 */
public record ArchiveDatabaseSummary(int numberOfParameters, int numberOfModuleInstanceIds, int numberOfTimeSeries) implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseSummary {}
