package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.utils.ThreadingUtil;
import nl.wldelft.fews.castor.TimeSeriesSetsComplexType;
import nl.wldelft.fews.castor.TimeStepComplexType;
import nl.wldelft.fews.common.config.CastorUtils;
import nl.wldelft.fews.system.data.config.DataStoreCastorUtils;
import nl.wldelft.fews.system.data.config.region.TimeSteps;
import nl.wldelft.util.timeseries.TimeStep;
import nl.wldelft.util.timeseries.TimeStepUtils;
import org.exolab.castor.xml.Unmarshaller;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TimeSeriesSetsUtil {

	@SuppressWarnings("unused")
	private static final Object o = CastorUtils.SCHEMA_FACTORY;

	/**
	 * Static Class
	 */
	private TimeSeriesSetsUtil() {}

	/**
	 * @param netcdfFile netcdfFile
	 * @return JSONArray
	 */
	public static JSONArray getDecomposedTimeSeriesSets(File netcdfFile){
		try {
			try (NetcdfFile dataSet = NetcdfFile.open(netcdfFile.getAbsolutePath())) {
				JSONArray decomposedTimeSeriesSets = new JSONArray();
				JSONArray allLocationIds = getAllLocationIds(dataSet);
				JSONArray stationIds = getStationIds(dataSet, allLocationIds);
				JSONObject locationIdToStationId = getLocationIdToStationId(stationIds, allLocationIds);

				dataSet.getVariables().stream().filter(s -> s.findAttribute("timeseries_sets_xml") != null).forEach(ThreadingUtil.throwing(t -> {
					JSONArray timeSeriesSets = getTimeSeriesSets(t);
					TimeSeriesSetsComplexType timeSeriesSetsComplexType = (TimeSeriesSetsComplexType) Unmarshaller.unmarshal(TimeSeriesSetsComplexType.class, new StringReader(t.findAttribute("timeseries_sets_xml").getStringValue()));

					if(timeSeriesSetsComplexType.getTimeSeriesSetCount() != timeSeriesSets.length())
						throw new IndexOutOfBoundsException(String.format("timeSeriesSetsComplexType.getTimeSeriesSetCount(%s) != timeSeriesSet.length(%s)", timeSeriesSetsComplexType.getTimeSeriesSetCount(), timeSeriesSets.length()));

					for (int i = 0; i < timeSeriesSets.length(); i++) {
						TimeStepComplexType ts = timeSeriesSetsComplexType.getTimeSeriesSet(i).getTimeStep();
						TimeStep timeStep = ts.getId() == null ? DataStoreCastorUtils.createTimeStepFromCastor(ts, TimeSteps.NONE, TimeZone.getTimeZone("GMT"), null) : TimeStepUtils.decode(ts.getId());

						JSONObject timeSeriesSet = timeSeriesSets.getJSONObject(i);
						timeSeriesSet.put("encodedTimeStepId", timeStep.getEncoded());
						timeSeriesSet.put("timeStepLabel", timeStep.toString());
						timeSeriesSet.put("minimumStepMillis", timeStep.getMinimumStepMillis());
						timeSeriesSet.put("maximumStepMillis", timeStep.getMaximumStepMillis());
						JSONArray decomposedSets = getDecomposedTimeSeriesSets(timeSeriesSet, locationIdToStationId);
						IntStream.range(0, decomposedSets.length()).forEachOrdered(j -> decomposedTimeSeriesSets.put(decomposedSets.getJSONObject(j)));
					}
				}));
				return decomposedTimeSeriesSets;
			}
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param timeSeriesSet timeSeriesSet
	 * @param locationIdToStationId locationIdToStationId
	 * @return JSONArray
	 */
	private static JSONArray getDecomposedTimeSeriesSets(JSONObject timeSeriesSet, JSONObject locationIdToStationId){
		JSONArray decomposedTimeSeriesSets = new JSONArray();

		JSONArray decomposedTimeSeriesSetToSingleLocationIds = getDecomposedTimeSeriesSetToSingleLocationIds(timeSeriesSet, locationIdToStationId);
		IntStream.range(0, decomposedTimeSeriesSetToSingleLocationIds.length()).forEachOrdered(i ->
				decomposedTimeSeriesSets.putAll(getDecomposedTimeSeriesSetToSingleEnsembleMemberIds(decomposedTimeSeriesSetToSingleLocationIds.getJSONObject(i))));

		return decomposedTimeSeriesSets;
	}

	/**
	 *
	 * @param timeSeriesSet timeSeriesSet
	 * @return JSONArray
	 */
	private static JSONArray getDecomposedTimeSeriesSetToSingleEnsembleMemberIds(JSONObject timeSeriesSet){
		if(!timeSeriesSet.has("ensembleMemberId"))
			return new JSONArray(List.of(timeSeriesSet));

		JSONArray decomposedTimeSeriesSets = new JSONArray();
		JSONArray ensembleMemberIds = timeSeriesSet.get("ensembleMemberId") instanceof JSONArray ? timeSeriesSet.getJSONArray("ensembleMemberId") : new JSONArray(List.of(timeSeriesSet.get("ensembleMemberId")));
		for (int i = 0; i < ensembleMemberIds.length(); i++) {
			JSONObject timeSeriesSetDeepClone = new JSONObject(timeSeriesSet.toString());
			timeSeriesSetDeepClone.put("ensembleMemberId", ensembleMemberIds.get(i).toString());
			timeSeriesSetDeepClone.put("ensembleMemberIndex", i);
			decomposedTimeSeriesSets.put(timeSeriesSetDeepClone);
		}
		return decomposedTimeSeriesSets;
	}

	/**
	 *
	 * @param timeSeriesSet timeSeriesSet
	 * @param locationIdToStationId locationIdToStationId
	 * @return JSONArray
	 */
	private static JSONArray getDecomposedTimeSeriesSetToSingleLocationIds(JSONObject timeSeriesSet, JSONObject locationIdToStationId){
		JSONArray decomposedTimeSeriesSets = new JSONArray();
		JSONArray locationIds = getLocationIds(timeSeriesSet);

		for (int i = 0; i < locationIds.length(); i++) {
			if(locationIds.getString(i).equals("dummyLocationId"))
				continue;
			JSONObject clone = new JSONObject(timeSeriesSet.toString());
			clone.put("locationId", locationIds.getString(i));
			clone.put("stationId", locationIdToStationId.getJSONObject(locationIds.getString(i)).getString("stationId"));
			clone.put("stationName", locationIdToStationId.getJSONObject(locationIds.getString(i)).getString("stationName"));
			decomposedTimeSeriesSets.put(clone);
		}
		return decomposedTimeSeriesSets;
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @param allLocationIds allLocationIds
	 * @return JSONArray
	 */
	private static JSONArray getStationIds(NetcdfFile dataSet, JSONArray allLocationIds){
		try {
			JSONArray stationIds = new JSONArray();
			if(dataSet.findDimension("stations") == null){
				for (int i = 0; i < allLocationIds.length(); i++) {
					stationIds.put(new JSONObject(Map.of(
							"stationId", allLocationIds.getString(i),
							"stationName", allLocationIds.getString(i))));
				}
			}
			else {
				for (int i = 0; i < dataSet.findDimension("stations").getLength(); i++) {
					stationIds.put(new JSONObject(Map.of(
							"stationId", dataSet.findVariable("station_id").read(String.format("%s,:", i)).toString(),
							"stationName", dataSet.findVariable("station_names").read(String.format("%s,:", i)).toString())));
				}
			}
			return stationIds;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param variable variable
	 * @return JSONArray
	 */
	private static JSONArray getTimeSeriesSets(Variable variable){
		JSONObject j = XML.toJSONObject(variable.findAttribute("timeseries_sets_xml").getStringValue()).getJSONObject("timeSeriesSets");
		return j.get("timeSeriesSet") instanceof JSONArray ? j.getJSONArray("timeSeriesSet") : new JSONArray(List.of(j.getJSONObject("timeSeriesSet")));
	}

	/**
	 *
	 * @param timeSeriesSet timeSeriesSet
	 * @return JSONArray
	 */
	private static JSONArray getLocationIds(JSONObject timeSeriesSet){
		return timeSeriesSet.get("locationId") instanceof JSONArray ? timeSeriesSet.getJSONArray("locationId") : new JSONArray(List.of(timeSeriesSet.getString("locationId")));
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @return JSONArray
	 */
	private static JSONArray getAllLocationIds(NetcdfFile dataSet){
		JSONArray allLocationIds = new JSONArray();

		dataSet.getVariables().stream().filter(s -> s.findAttribute("timeseries_sets_xml") != null).forEachOrdered(variable -> {
			JSONArray timeSeriesSets = getTimeSeriesSets(variable);

			if(IntStream.range(0, timeSeriesSets.length()).anyMatch(k -> timeSeriesSets.getJSONObject(k).getString("moduleInstanceId").equals("dummyModuleInstanceId"))){
				IntStream.range(0, timeSeriesSets.length()).filter(i -> !getLocationIds(timeSeriesSets.getJSONObject(i)).getString(0).equals("dummyLocationId")).forEachOrdered(i ->
					allLocationIds.put(i, getLocationIds(timeSeriesSets.getJSONObject(i)).getString(0))
				);
			}
			else if(IntStream.range(0, timeSeriesSets.length()).anyMatch(k -> getLocationIds(timeSeriesSets.getJSONObject(k)).toList().contains("dummyLocationId"))) {
				IntStream.range(0, timeSeriesSets.length()).forEachOrdered(j -> {
					JSONArray locationIds = getLocationIds(timeSeriesSets.getJSONObject(j));
					IntStream.range(0, locationIds.length()).filter(i -> !locationIds.getString(i).equals("dummyLocationId") && allLocationIds.isNull(i)).forEachOrdered(i -> allLocationIds.put(i, locationIds.getString(i)));
				});
			}
			else {
				IntStream.range(0, timeSeriesSets.length()).forEachOrdered(j -> {
					JSONArray locationIds = getLocationIds(timeSeriesSets.getJSONObject(j));
					IntStream.range(0, locationIds.length()).filter(i -> !locationIds.getString(i).equals("dummyLocationId") && !allLocationIds.toList().contains(locationIds.get(i))).forEachOrdered(i -> allLocationIds.put(locationIds.getString(i)));
				});
			}
		});
		return allLocationIds;
	}

	/**
	 *
	 * @param stationIds stationIds
	 * @param allLocationIds allLocationIds
	 * @return JSONObject
	 */
	private static JSONObject getLocationIdToStationId(JSONArray stationIds, JSONArray allLocationIds){
		JSONObject locationIdToStationId = new JSONObject();

		if(stationIds.length() != allLocationIds.length())
			stationIds = new JSONArray(stationIds.toList().stream().distinct().collect(Collectors.toList()));

		if(stationIds.length() != allLocationIds.length())
			throw new IndexOutOfBoundsException(String.format("stationIds.length(%s) != allLocationIds.length(%s)", stationIds.length(), allLocationIds.length()));

		for (int i = 0; i < stationIds.length(); i++)
			locationIdToStationId.put(allLocationIds.getString(i), stationIds.getJSONObject(i));

		return locationIdToStationId;
	}
}