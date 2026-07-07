package nl.fews.archivedatabase.common.query;

import nl.fews.archivedatabase.common.query.interfaces.Read;
import nl.fews.archivedatabase.common.query.interfaces.Summarize;
import nl.fews.archivedatabase.common.query.operations.Filter;
import nl.fews.archivedatabase.common.shared.database.Collection;
import nl.fews.archivedatabase.common.shared.database.Database;
import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.enums.TimeSeriesType;
import nl.fews.archivedatabase.common.shared.utils.TimeSeriesTypeUtil;
import nl.wldelft.fews.system.data.config.region.TimeSeriesValueType;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.util.timeseries.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class ArchiveDatabaseGuiDataReader implements nl.fews.archivedatabase.interfaces.ArchiveDatabaseGuiDataReader, AutoCloseable {

	/**
	 *
	 */
	private static final String BASE_NAMESPACE = "nl.fews.archivedatabase.common";

	/**
	 *
	 */
	private static ArchiveDatabaseGuiDataReader archiveDatabaseGuiDataReader = null;

	/**
	 *
	 */
	private static final Object mutex = new Object();

	/**
	 * Creates a new instance of this interface implementation
	 */
	public static ArchiveDatabaseGuiDataReader create() {
		if(archiveDatabaseGuiDataReader == null)
			archiveDatabaseGuiDataReader = new ArchiveDatabaseGuiDataReader();
		return archiveDatabaseGuiDataReader;
	}

	/**
	 *
	 */
	public void close() {
		synchronized (mutex) {
			ArchiveDatabaseGuiDataReader.archiveDatabaseGuiDataReader = null;
		}
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseReadResult
	 */
	@Override
	public nl.fews.archivedatabase.common.query.ArchiveDatabaseReadResult read(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		if(archiveDatabaseResultSearchParameters.getPeriod().getEndDate().before(archiveDatabaseResultSearchParameters.getPeriod().getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		try{
			var timeSeriesType = !nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL.equals(archiveDatabaseResultSearchParameters.getTimeSeriesType()) ?
				TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType()) :
				TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED;
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			var query = new LinkedHashMap<String, List<Object>>();

			query.put("metaData.areaId", List.of(archiveDatabaseResultSearchParameters.getAreaId()));

			if(archiveDatabaseResultSearchParameters.getSourceIds() != null && !archiveDatabaseResultSearchParameters.getSourceIds().isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getSourceIds()));

			if(archiveDatabaseResultSearchParameters.getParameterIds() != null && !archiveDatabaseResultSearchParameters.getParameterIds().isEmpty())
				query.put("parameterId", new ArrayList<>(archiveDatabaseResultSearchParameters.getParameterIds()));

			if(archiveDatabaseResultSearchParameters.getModuleInstanceIds() != null && !archiveDatabaseResultSearchParameters.getModuleInstanceIds().isEmpty())
				query.put("moduleInstanceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getModuleInstanceIds()));

			if(archiveDatabaseResultSearchParameters.getTimeSteps() != null && !archiveDatabaseResultSearchParameters.getTimeSteps().isEmpty())
				query.put("encodedTimeStepId", new ArrayList<>(archiveDatabaseResultSearchParameters.getTimeSteps().stream().map(TimeStep::getEncoded).toList()));

			var read = (Read)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Read%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			var result = read.read(collection, query, archiveDatabaseResultSearchParameters.getPeriod().getStartDate(), archiveDatabaseResultSearchParameters.getPeriod().getEndDate()).iterator();

			return new nl.fews.archivedatabase.common.query.ArchiveDatabaseReadResult(result, TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType());
		}
		catch(Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param archiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters
	 * @return ArchiveDatabaseSummary
	 */
	@Override
	public nl.fews.archivedatabase.common.query.ArchiveDatabaseSummary getSummary(ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters) {
		if(archiveDatabaseResultSearchParameters.getPeriod().getEndDate().before(archiveDatabaseResultSearchParameters.getPeriod().getStartDate()))
			throw new IllegalArgumentException("End of period must fall on or after start of period");

		try{
			var timeSeriesType = !nl.wldelft.fews.system.data.timeseries.TimeSeriesType.SIMULATED_HISTORICAL.equals(archiveDatabaseResultSearchParameters.getTimeSeriesType()) ?
				TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, archiveDatabaseResultSearchParameters.getTimeSeriesType()) :
				TimeSeriesType.SCALAR_SIMULATED_HISTORICAL_STITCHED;
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			var query = new LinkedHashMap<String, List<Object>>();

			query.put("metaData.areaId", List.of(archiveDatabaseResultSearchParameters.getAreaId()));

			if(archiveDatabaseResultSearchParameters.getSourceIds() != null && !archiveDatabaseResultSearchParameters.getSourceIds().isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getSourceIds()));

			if(archiveDatabaseResultSearchParameters.getParameterIds() != null && !archiveDatabaseResultSearchParameters.getParameterIds().isEmpty())
				query.put("parameterId", new ArrayList<>(archiveDatabaseResultSearchParameters.getParameterIds()));

			if(archiveDatabaseResultSearchParameters.getModuleInstanceIds() != null && !archiveDatabaseResultSearchParameters.getModuleInstanceIds().isEmpty())
				query.put("moduleInstanceId", new ArrayList<>(archiveDatabaseResultSearchParameters.getModuleInstanceIds()));

			if(archiveDatabaseResultSearchParameters.getTimeSteps() != null && !archiveDatabaseResultSearchParameters.getTimeSteps().isEmpty())
				query.put("encodedTimeStepId", new ArrayList<>(archiveDatabaseResultSearchParameters.getTimeSteps().stream().map(TimeStep::getEncoded).toList()));

			var bucketKeys = List.of("bucketSize", "bucket");
			var collectionKeys = Index.getKeys(collection);
			var countKeys = collectionKeys.stream().anyMatch(bucketKeys::contains) ? collectionKeys.stream().filter(s -> !bucketKeys.contains(s)).toList() : List.<String>of();

			var distinctKeyFields = Map.of(
			"parameterId", List.of("parameterId"),
			"moduleInstanceId", List.of("moduleInstanceId"),
			"numberOfTimeSeries", countKeys);

			var summarize = (Summarize)Class.forName(String.format("%s.%s.%s", BASE_NAMESPACE, "query.operations", String.format("Summarize%s", TimeSeriesTypeUtil.getTimeSeriesTypeTypes(timeSeriesType)))).getConstructor().newInstance();
			var summary = summarize.getSummary(collection, distinctKeyFields, query, archiveDatabaseResultSearchParameters.getPeriod().getStartDate(), archiveDatabaseResultSearchParameters.getPeriod().getEndDate());

			return new nl.fews.archivedatabase.common.query.ArchiveDatabaseSummary(summary.get("parameterId"), summary.get("moduleInstanceId"), summary.get("numberOfTimeSeries"));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param areaId areaId
	 * @param fewsTimeSeriesType fewsTimeSeriesType
	 * @param sourceIds sourceIds
	 * @return ArchiveDatabaseFilterOptions
	 */
	@Override
	public nl.fews.archivedatabase.common.query.ArchiveDatabaseFilterOptions getFilterOptions(String areaId, nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType, Set<String> sourceIds) {

		try{
			var timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);
			var fields = Map.of("moduleInstanceId", String.class, "parameterId", String.class, "encodedTimeStepId", String.class);
			var query = new LinkedHashMap<String, List<Object>>();
			query.put("metaData.areaId", List.of(areaId));
			if(sourceIds != null && !sourceIds.isEmpty())
				query.put("metaData.sourceId", new ArrayList<>(sourceIds));

			var filters = Filter.getFilters(collection, fields, query);
			return new nl.fews.archivedatabase.common.query.ArchiveDatabaseFilterOptions(
				filters.get("parameterId").stream().map(Object::toString).sorted().collect(Collectors.toCollection(LinkedHashSet::new)),
				filters.get("moduleInstanceId").stream().map(Object::toString).sorted().collect(Collectors.toCollection(LinkedHashSet::new)),
				filters.get("encodedTimeStepId").stream().map(s -> TimeStepUtils.decode(s.toString())).sorted(Comparator.comparing(Object::toString)).collect(Collectors.toCollection(LinkedHashSet::new))
			);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param fewsTimeSeriesType fewsTimeSeriesType
	 * @return ArchiveDatabaseFilterOptions
	 */
	@Override
	public Set<String> getSourceIds(nl.wldelft.fews.system.data.timeseries.TimeSeriesType fewsTimeSeriesType) {
		try{
			var timeSeriesType = TimeSeriesTypeUtil.getTimeSeriesTypeByFewsTimeSeriesType(TimeSeriesValueType.SCALAR, fewsTimeSeriesType);
			var collection = TimeSeriesTypeUtil.getTimeSeriesTypeCollection(timeSeriesType);

			return new HashSet<>(Database.instance().distinct(Collection.TimeSeriesIndex.toString(), "sourceId", Map.of("collection", collection), String.class));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
