package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.fews.archivedatabase.mongodb.shared.utils.DateUtil;
import nl.fews.archivedatabase.mongodb.shared.utils.PathUtil;
import nl.wldelft.fews.common.config.CastorUtils;
import nl.wldelft.netcdf.NetcdfTimeSeriesParser;
import nl.wldelft.util.timeseries.SimpleTimeSeriesContentHandler;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import nl.wldelft.util.timeseries.TimeSeriesHeader;
import org.bson.Document;
import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.units.DateUnit;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("unchecked")
public final class NetcdfUtil {

	@SuppressWarnings("unused")
	private static final Object o = CastorUtils.SCHEMA_FACTORY;

	/**
	 * Static Class
	 */
	private NetcdfUtil() {}

	/**
	 *
	 * @param metaData metaData
	 * @return Map<File, Date>
	 */
	public static Map<File, Pair<Date, JSONObject>> getExistingNetcdfFilesFs(JSONObject metaData) {
		Map<File, Pair<Date, JSONObject>> existingNetcdfFilesFs = new HashMap<>();
		Object n = metaData.getJSONObject(metaData.getString("metaDataType")).has("netcdf") ? metaData.getJSONObject(metaData.getString("metaDataType")).get("netcdf") : null;
		JSONArray netcdfs = n != null ? n instanceof JSONObject ? new JSONArray(List.of((JSONObject) n)) : (JSONArray) n : new JSONArray();
		for (int i = 0; i < netcdfs.length(); i++) {
			JSONObject netcdf = netcdfs.getJSONObject(i);
			File netcdfFile = PathUtil.normalize(new File(metaData.getString("parentFilePath"), netcdf.getString("relativeFilePath")));
			if(netcdfFile.exists())
				existingNetcdfFilesFs.put(netcdfFile, new Pair<>(new Date(netcdfFile.lastModified()), netcdf));
		}
		return existingNetcdfFilesFs;
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @return List<TimeSeriesArray<TimeSeriesHeader>>
	 */
	public static List<Document> getTimeSeriesArraysAsDocuments(File netcdfFile){
		try {
			SimpleTimeSeriesContentHandler timeSeriesContentHandler = new SimpleTimeSeriesContentHandler();
			timeSeriesContentHandler.setTimeSeriesType(TimeSeriesArray.Type.SCALAR);
			new NetcdfTimeSeriesParser(NetcdfTimeSeriesParser.NETCDF_SCALAR).parse(netcdfFile, timeSeriesContentHandler);

			TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = timeSeriesContentHandler.getTimeSeriesArrays();
			timeSeriesArrays.removeCompletelyMissing();

			List<Document> timeSeriesDocuments = new ArrayList<>();

			Arrays.stream(timeSeriesArrays.toArray()).forEachOrdered(timeSeriesArray -> {
				TimeSeriesHeader timeSeriesHeader = timeSeriesArray.getHeader();

				List<Date> timeSeriesDates = Arrays.stream(DateUtil.getDates(timeSeriesArray.toTimesArray())).collect(Collectors.toList());
				float[] timeSeriesValues = timeSeriesArray.toFloatArray();

				int[] timeSeriesFlags = new int[timeSeriesDates.size()];
				for (int i = 0; i < timeSeriesFlags.length; i++)
					timeSeriesFlags[i] = timeSeriesArray.getFlag(i);

				List<String> timeSeriesComments = IntStream.range(0, timeSeriesDates.size()).boxed().map(timeSeriesArray::getComment).collect(Collectors.toList());
				String unit = timeSeriesHeader.getUnit();
				Date forecastTime = new Date(timeSeriesHeader.getForecastTime());
				String stationId = timeSeriesHeader.getLocationId();
				String stationName = "";

				timeSeriesDocuments.add(getTimeSeriesDocument(timeSeriesDates, timeSeriesValues, timeSeriesFlags, timeSeriesComments, unit, forecastTime, stationId, stationName));
			});
			return timeSeriesDocuments;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param netcdfFile netcdfFile
	 * @return JSONArray
	 */
	public static List<Document> getTimeSeriesDocuments(File netcdfFile){
		try {
			try (NetcdfFile dataSet = NetcdfFile.openInMemory(netcdfFile.getAbsolutePath())) {
				List<Document> timeSeriesDocuments = new ArrayList<>();
				dataSet.getVariables().stream().filter(s -> s.findAttribute("timeseries_sets_xml") != null).forEach(variable -> timeSeriesDocuments.addAll(getTimeSeriesDocuments(variable, dataSet)));
				return timeSeriesDocuments;
			}
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param valuesVariable values
	 * @param dataSet dataSet
	 * @return List<Document>
	 */
	private static List<Document> getTimeSeriesDocuments(Variable valuesVariable, NetcdfFile dataSet){
		try {
			valuesVariable.setCachedData(valuesVariable.read());
			List<Document> timeSeriesDocuments = new ArrayList<>();

			Dimension station = valuesVariable.getDimension(valuesVariable.findDimensionIndex("stations"));
			Dimension realization = valuesVariable.getDimension(valuesVariable.findDimensionIndex("realization"));

			Variable flags = getFlags(valuesVariable, dataSet);
			Variable commentIds = getCommentIds(valuesVariable, dataSet);

			List<Date> timeSeriesDates = getTimeSeriesDates(valuesVariable, dataSet);
			List<String> timeSeriesStationIds = getTimeSeriesStationIds(dataSet);
			List<String> timeSeriesStationNames = getTimeSeriesStationNames(dataSet);

			Float fillValue = getFillValue(valuesVariable);
			Float scaleFactor = getScaleFactor(valuesVariable);
			Float addOffset = getAddOffset(valuesVariable);
			String unit = getUnits(valuesVariable);

			Date forecastTime = getForecastTime(dataSet);

			String selectionTemplate = station == null && realization == null ? ":" : realization == null ? ":,%2$s" : ":,%1$s,%2$s";
			int stationLength = station == null ? 1 : station.getLength();
			int realizationLength = realization == null ? 1 : realization.getLength();

			for (int s = 0; s < stationLength; s++) {

				String locationId = station == null ? getLocationId(valuesVariable) : null;
				String stationId = station == null ? locationId : timeSeriesStationIds.get(s);
				String stationName = station == null ? locationId : timeSeriesStationNames.get(s);

				for (int r = 0; r < realizationLength; r++) {

					String selection = String.format(selectionTemplate, r, s);

					float[] timeSeriesValues = getTimeSeriesValues(valuesVariable.read(selection), fillValue, scaleFactor, addOffset);
					if(timeSeriesValues.length == 0)
						continue;
					int[] timeSeriesFlags = getTimeSeriesFlags(flags, selection);
					List<String> timeSeriesComments = getTimeSeriesComments(dataSet, commentIds, selection);

					if(timeSeriesValues.length != timeSeriesDates.size())
						throw new IndexOutOfBoundsException(String.format("timeSeriesValues.size(%s) != timeSeriesDates.size(%s)", timeSeriesValues.length, timeSeriesDates.size()));
					timeSeriesDocuments.add(getTimeSeriesDocument(timeSeriesDates, timeSeriesValues, timeSeriesFlags, timeSeriesComments, unit, forecastTime, stationId, stationName));
				}
			}
			return timeSeriesDocuments;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param timeSeriesDates timeSeriesDates
	 * @param timeSeriesValues timeSeriesValues
	 * @param timeSeriesFlags timeSeriesFlags
	 * @param timeSeriesComments timeSeriesComments
	 * @param unit into
	 * @param forecastTime forecastTime
	 * @param stationId stationId
	 * @param stationName stationName
	 * @return Document
	 */
	private static Document getTimeSeriesDocument(List<Date> timeSeriesDates, float[] timeSeriesValues, int[] timeSeriesFlags, List<String> timeSeriesComments, String unit, Date forecastTime, String stationId, String stationName){
		return new
				Document("timeseries", IntStream.range(0, timeSeriesDates.size()).boxed().map(i -> new
						Document("t", timeSeriesDates.get(i)).
						append("v", timeSeriesValues[i]).
						append("f", timeSeriesFlags[i]).
						append("c", timeSeriesComments.get(i))).collect(Collectors.toList())).
				append("unit", unit).
				append("forecastTime", forecastTime).
				append("stationId", stationId).
				append("stationName", stationName);
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @param commentIds commentIds
	 * @param selection selection
	 * @return List<String>
	 */
	private static List<String> getTimeSeriesComments(NetcdfFile dataSet, Variable commentIds, String selection){
		try {
			Variable comments = dataSet.findVariable("comments");
			List<String> timeSeriesComments = new ArrayList<>();
			for (int i = 0; i < comments.getDimension(0).getLength(); i++)
				timeSeriesComments.add(comments.read(String.format("%s,:", i)).toString());

			int[] commentIdIndices = (int[])commentIds.read(selection).get1DJavaArray(Integer.class);
			return Arrays.stream(commentIdIndices).boxed().map(i -> i >= 0 ? timeSeriesComments.get(i) : "").collect(Collectors.toList());
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param flags flags
	 * @param selection selection
	 * @return List<Date>
	 */
	private static int[] getTimeSeriesFlags(Variable flags, String selection){
		try {
			return (int[])flags.read(selection).get1DJavaArray(Integer.class);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param values values
	 * @param fillValue fillValue
	 * @param scaleFactor scaleFactor
	 * @param addOffset addOffset
	 * @return List<Date>
	 */
	private static float[] getTimeSeriesValues(Array values, Float fillValue, Float scaleFactor, Float addOffset){
		try {
			float[] timeSeriesValues = (float[])values.get1DJavaArray(Float.class);
			if(fillValue != null) {
				boolean allFillValue = true;
				for (int i = 0; i < timeSeriesValues.length; i++) {
					if (timeSeriesValues[i] != fillValue){
						allFillValue = false;
						timeSeriesValues[i] = timeSeriesValues[i] * scaleFactor + addOffset;
					}
					else
						timeSeriesValues[i] = Float.NaN;
				}
				if(allFillValue)
					return new float[]{};
			}
			return timeSeriesValues;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @return List<Date>
	 */
	private static List<String> getTimeSeriesStationIds(NetcdfFile dataSet){
		try {
			Variable stationIds = dataSet.findVariable("station_id");
			List<String> timeSeriesStationIds = new ArrayList<>();
			for (int i = 0; i < stationIds.getDimension(0).getLength(); i++)
				timeSeriesStationIds.add(stationIds.read(String.format("%s,:", i)).toString());
			return timeSeriesStationIds;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @return List<Date>
	 */
	private static List<String> getTimeSeriesStationNames(NetcdfFile dataSet){
		try {
			Variable stationNames = dataSet.findVariable("station_names");
			List<String> timeSeriesStationIds = new ArrayList<>();
			for (int i = 0; i < stationNames.getDimension(0).getLength(); i++)
				timeSeriesStationIds.add(stationNames.read(String.format("%s,:", i)).toString());
			return timeSeriesStationIds;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @param dataSet dataSet
	 * @return List<Date>
	 */
	private static List<Date> getTimeSeriesDates(Variable valuesVariable, NetcdfFile dataSet){
		try {
			Variable time = dataSet.findVariable(valuesVariable.getDimension(0).getFullNameEscaped());
			time.setCachedData(time.read());

			List<Date> timeSeriesDates = new ArrayList<>();
			DateUnit dateUnit = new DateUnit(time.findAttribute("units").getStringValue());
			Array times = time.read();
			IntStream.range(0, (int)times.getSize()).forEachOrdered(i -> timeSeriesDates.add(dateUnit.makeDate(times.getDouble(i))));
			return timeSeriesDates;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @param dataSet dataSet
	 * @return Variable
	 */
	private static Variable getFlags(Variable valuesVariable, NetcdfFile dataSet){
		try{
			Variable flags = dataSet.findVariable(String.format("%s_status_flag", valuesVariable.getFullNameEscaped()));
			flags.setCachedData(flags.read());
			return flags;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @param dataSet dataSet
	 * @return Variable
	 */
	private static Variable getCommentIds(Variable valuesVariable, NetcdfFile dataSet){
		try {
			Variable commentIds = dataSet.findVariable(String.format("%s_comment_id", valuesVariable.getFullNameEscaped()));
			commentIds.setCachedData(commentIds.read());
			return commentIds;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param dataSet dataSet
	 * @return Date
	 */
	private static Date getForecastTime(NetcdfFile dataSet){
		try {
			Variable analysisTime = dataSet.findVariable("analysis_time");
			return analysisTime != null ? new DateUnit(analysisTime.findAttribute("units").getStringValue()).makeDate(analysisTime.readScalarDouble()) : null;
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @return String
	 */
	private static String getUnits(Variable valuesVariable){
		return valuesVariable.findAttribute("units").getStringValue();
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @return String
	 */
	private static String getLocationId(Variable valuesVariable){
		Attribute locationId = valuesVariable.findAttribute("location_id");
		return locationId != null ? locationId.getStringValue() : null;
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @return Float
	 */
	private static Float getFillValue(Variable valuesVariable){
		Attribute fillValue = valuesVariable.findAttribute("_FillValue");
		return fillValue != null ? fillValue.getNumericValue().floatValue() : null;
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @return Float
	 */
	private static Float getScaleFactor(Variable valuesVariable){
		Attribute scaleFactor = valuesVariable.findAttribute("scale_factor");
		return scaleFactor != null ? scaleFactor.getNumericValue().floatValue() : 1.0f;
	}

	/**
	 *
	 * @param valuesVariable valuesVariable
	 * @return Float
	 */
	private static Float getAddOffset(Variable valuesVariable){
		Attribute addOffset = valuesVariable.findAttribute("add_offset");
		return addOffset != null ? addOffset.getNumericValue().floatValue() : 0f;
	}
}