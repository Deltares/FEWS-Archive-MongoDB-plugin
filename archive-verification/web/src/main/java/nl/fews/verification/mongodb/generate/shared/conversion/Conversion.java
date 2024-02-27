package nl.fews.verification.mongodb.generate.shared.conversion;

import org.bson.Document;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class Conversion {
	private Conversion(){}

	/**
	 * Retrieves the corresponding SQL data type for the given BSON data type.
	 *
	 * @param bsonType the BSON data type
	 * @return the corresponding SQL data type
	 */
	public static String getSqlType(String bsonType){
		return switch(bsonType){
			case "string" -> "varchar";
			case "bool" -> "boolean";
			case "int", "int64" -> "int";
			case "float64" -> "float";
			case "bson.Decimal128" -> "decimal";
			default -> bsonType;
		};
	}

	/**
	 * Retrieves the BSON data type for the given Java data type.
	 *
	 * @param javaType the Java data type
	 * @return the corresponding BSON data type
	 */
	public static String getBsonType(String javaType){
		return switch(javaType){
			case "String" -> "string";
			case "Boolean" -> "bool";
			case "Integer" -> "int";
			case "Int64" -> "int64";
			case "Double" -> "float64";
			case "Decimal" -> "bson.Decimal128";
			default -> javaType;
		};
	}

	/**
	 * Retrieves the corresponding cube data type for the given Java data type.
	 *
	 * @param javaType the Java data type
	 * @return the corresponding cube data type
	 */
	public static String getCubeType(String javaType){
		return switch(javaType){
			case "String" -> "string";
			case "Boolean" -> "boolean";
			case "Integer", "Int64" -> "int64";
			case "Double", "Decimal" -> "double";
			default -> javaType;
		};
	}

	/**
	 * Returns a formatted string representation of the given filter document.
	 *
	 * @param filter the filter document
	 * @return the formatted string representation of the filter document
	 */
	public static String getFilter(Document filter){
		return String.format("\n        {\n%s\n        }", filter.entrySet().stream().map(e -> String.format("          \"%s\": \"%s\"", e.getKey(), e.getValue().toString().replace("\"", "\\\""))).collect(Collectors.joining(",\n")));
	}

	/**
	 * Returns the forecast time based on the given time.
	 *
	 * @param time the time value
	 * @return the forecast time
	 */
	public static String getForecastTime(String time){
		return time.equals("local") ? "localForecastTime" : "forecastTime";
	}

	/**
	 * Retrieves the event time based on the given time. Returns "lt" if the time is "local", otherwise returns "t".
	 *
	 * @param time the time value
	 * @return the event time
	 */
	public static String getEventTime(String time){
		return time.equals("local") ? "lt" : "t";
	}

	/**
	 * Retrieves the start time based on the given time. Returns "localStartTime" if the time is "local", otherwise returns "startTime".
	 *
	 * @param time the time value
	 * @return the start time
	 */
	public static String getStartTime(String time){
		return time.equals("local") ? "localStartTime" : "startTime";
	}

	/**
	 * Retrieves the end time based on the given time. Returns "localEndTime" if the time is "local", otherwise returns "endTIme".
	 *
	 * @param time the time value
	 * @return the end time
	 */
	public static String getEndTime(String time){
		return time.equals("local") ? "localEndTime" : "endTime";
	}

	/**
	 * Retrieves the event value based on the given time. If the time is "display", returns "dv", otherwise returns "v".
	 *
	 * @param time the time value
	 * @return the event value
	 */
	public static String getEventValue(String time){
		return time.equals("display") ? "dv" : "v";
	}

	/**
	 * Retrieves the seasonalities for forecasting based on the given list of seasonalities.
	 *
	 * @param seasonalities the list of seasonalities
	 * @return the formatted string representation of the seasonalities for forecasting
	 */
	public static String getSeasonalities(List<Document> seasonalities){
		return seasonalities.stream().map(
			s -> String.format("        \"%sSeason\": {\"$switch\": {\"branches\": [\n%s\n          ]}}", s.getString("Name"), s.getList("Breakpoint", Document.class).stream().map(
				b -> String.format("          {\"case\": {\"$%s\": [\"$monthDay\", \"%s\"]}, \"then\": \"%s\"}", b.getString("Operator"), b.getString("Threshold"), b.getString("Name"))).collect(Collectors.joining(",\n")))).collect(Collectors.joining(",\n"));
	}

	/**
	 * Retrieves the location mapping as a string representation of a MongoDB aggregation pipeline.
	 *
	 * @param locationMapping the location mapping document containing the mapping details
	 * @return the location mapping pipeline string
	 */
	public static String getLocationMap(Document locationMapping){
		String cases = locationMapping.entrySet().stream().map(
			m -> String.format("            {\"case\": {\"$eq\": [\"$locationId\", \"%s\"]}, \"then\": \"%s\"}", m.getKey().replace("\"", "\\\""), m.getValue().toString().replace("\"", "\\\""))).collect(Collectors.joining(",\n"));
		return cases.isEmpty() ? "$locationId" : String.format("{\"$switch\":\n          {\"branches\": [\n%s\n          ],\n          \"default\": \"$locationId\"}}", cases);
	}

	/**
	 * Retrieves the formatted string representation of the seasonalities for forecasting
	 * based on the given list of seasonalities.
	 *
	 * @param seasonalities the list of seasonalities
	 * @return the formatted string representation of the seasonalities for forecasting
	 */
	public static String getSeasonalityColumns(List<String> seasonalities){
		return seasonalities.stream().map(s -> String.format("    - Name: %sSeason\n      MongoType: string\n      SqlName: %sSeason\n      SqlType: varchar", s, s)).collect(Collectors.joining("\n"));
	}

	/**
	 * Returns a DateTimeFormatter object for formatting dates to the pattern "yyyy-MM".
	 *
	 * @return the DateTimeFormatter object
	 */
	public static DateTimeFormatter getMonthDateTimeFormatter(){
		return DateTimeFormatter.ofPattern("yyyy-MM");
	}

	/**
	 * Retrieves the attribute types of locations based on the specified location attributes.
	 *
	 * @param locations            the document containing location details
	 * @param locationAttributes   the document containing location attribute details
	 * @return the document containing attribute types of locations
	 */
	public static Document getLocationAttributeTypes(Document locations, Document locationAttributes){
		Document r = new Document();
		Map<String, Object> locationAttributesMap = locationAttributes.getList("Attributes", String.class).stream().collect(Collectors.toMap(a -> a, a -> a));
		locations.get("locations", Document.class).values().forEach(v -> {
			r.putAll(((Document)v).entrySet().stream().filter(a -> !a.getKey().equals("attributes") && locationAttributesMap.containsKey(a.getKey())).collect(Collectors.toMap(Map.Entry::getKey, a -> a.getValue().getClass().getSimpleName())));
			r.putAll(((Document)v).get("attributes", Document.class).entrySet().stream().filter(a -> locationAttributesMap.containsKey(a.getKey())).collect(Collectors.toMap(Map.Entry::getKey, a -> ((Document)a.getValue()).get("value").getClass().getSimpleName())));
		});
		r.put("locationId", "String");
		return r;
	}

	/**
	 * Retrieves the observed class based on the given class document.
	 *
	 * @param _class the class document containing the class details
	 * @return the observed class as a string representation
	 */
	public static String getObservedClass(Document _class){
		return Conversion.getClass(_class, "observed");
	}

	/**
	 * Retrieves the forecast class based on the given class document.
	 *
	 * @param _class the class document containing the class details
	 * @return the forecast class as a string representation
	 */
	public static String getForecastClass(Document _class){
		return Conversion.getClass(_class, "forecast");
	}

	/**
	 * Returns a string representation of the class based on the provided parameters.
	 *
	 * @param _class    The class document containing information about the class.
	 * @param classType The type of the class.
	 *
	 * @return A string representation of the class based on the provided parameters.
	 */
	private static String getClass(Document _class, String classType){
		if(_class == null){
			return "\"NA\"";
		}
		return String.format("{\"$switch\":\n          {\"branches\": [\n%s\n          ],\n          \"default\": \"undefinedLocation\"}}", _class.getList("Locations", Document.class).stream().map(
			l -> String.format("            {\"case\": {\"$or\": [{\"$eq\": [\"$location\", \"\"]}, {\"$eq\": [\"$location\", \"%s\"]}]}, \"then\": %s}", l.getString("Location"), String.format("{\"$switch\":\n              {\"branches\": [\n%s\n              ],\n              \"default\": \"undefinedValue\"}}", l.getList("Breakpoint", Document.class).stream().map(
				b -> String.format("                {\"case\": {\"$%s\": [\"$%s\", %s]}, \"then\": \"%s\"}", b.getString("Operator"), classType, b.get("Threshold"), b.getString("Name"))).collect(Collectors.joining(",\n"))))).collect(Collectors.joining(",\n")));
	}
}
