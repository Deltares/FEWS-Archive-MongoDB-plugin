package nl.fews.verification.mongodb.generate.shared.conversion;

import org.bson.Document;

import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class Conversion {
	private Conversion(){}

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

	public static String getCubeType(String javaType){
		return switch(javaType){
			case "String" -> "string";
			case "Boolean" -> "boolean";
			case "Integer", "Int64" -> "int64";
			case "Double", "Decimal" -> "double";
			default -> javaType;
		};
	}

	public static <T extends Comparable<T>> T max(T o1, T o2){
		if(o1 == null || o2 == null) return null;
		return o1.compareTo(o2) > 0 ? o1 : o2;
	}

	public static YearMonth getYearMonth(Date date){
		return YearMonth.from(date.toInstant().atZone(ZoneId.of("UTC")).toLocalDate());
	}

	public static Date getYearMonthDate(YearMonth yearMonth){
		return Date.from(yearMonth.atDay(1).atStartOfDay(ZoneId.of("UTC")).toInstant());
	}

	public static Date getYearMonthDate(String yearMonth){
		return getYearMonthDate(YearMonth.parse(yearMonth));
	}

	public static String getForecastTime(String time){
		return time.equals("local") ? "localForecastTime" : "forecastTime";
	}

	public static String getEventTime(String time){
		return time.equals("local") ? "lt" : "t";
	}

	public static String getStartTime(String time){
		return time.equals("local") ? "localStartTime" : "startTime";
	}

	public static String getEndTime(String time){
		return time.equals("local") ? "localEndTime" : "endTime";
	}

	public static String getEventValue(String time){
		return time.equals("display") ? "dv" : "v";
	}

	public static String getSeasonalities(List<Document> seasonalities){
		return seasonalities.stream().map(
			s -> String.format("  \"%sSeason\": {\"$switch\": {\"branches\": [\n%s\n    ]}}", s.getString("Name"), s.getList("Breakpoint", Document.class).stream().map(
				b -> String.format("    {\"case\": {\"$%s\": [\"$monthDay\", \"%s\"]}, \"then\": \"%s\"}", b.getString("Operator"), b.getString("Threshold"), b.getString("Name"))).collect(Collectors.joining(",\n")))).collect(Collectors.joining(",\n"));
	}

	public static String getLocationMap(Document locationMapping){
		String cases = locationMapping.entrySet().stream().map(
			m -> String.format("      {\"case\": {\"$eq\": [\"$locationId\", \"%s\"]}, \"then\": \"%s\"}", m.getKey().replace("\"", "\\\""), m.getValue().toString().replace("\"", "\\\""))).collect(Collectors.joining(",\n"));
		return cases.isEmpty() ? "\"$locationId\"" : String.format("{\"$switch\":\n    {\"branches\": [\n%s\n    ],\n    \"default\": \"$locationId\"}}", cases);
	}

	public static String getSeasonalityColumns(List<String> seasonalities){
		return seasonalities.stream().map(s -> String.format("    - Name: %sSeason\n      MongoType: string\n      SqlName: %sSeason\n      SqlType: varchar", s, s)).collect(Collectors.joining("\n"));
	}

	public static DateTimeFormatter getMonthDateTimeFormatter(){
		return DateTimeFormatter.ofPattern("yyyy-MM");
	}

	public static DateTimeFormatter getDateTimeFormatter(){
		return DateTimeFormatter.ofPattern("yyyy-MM-dd");
	}

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

	public static String getObservedClass(Document _class){
		return Conversion.getClass(_class, "observed");
	}

	public static String getForecastClass(Document _class){
		return Conversion.getClass(_class, "forecast");
	}

	private static String getClass(Document _class, String classType){
		if(_class == null){
			return "\"NA\"";
		}
		return String.format("{\"$switch\":\n          {\"branches\": [\n%s\n          ],\n          \"default\": \"undefinedLocation\"}}", _class.getList("Locations", Document.class).stream().map(
			l -> String.format("            {\"case\": {\"$or\": [{\"$eq\": [\"\", \"%s\"]}, {\"$eq\": [\"$location\", \"%s\"]}]}, \"then\": %s}", l.getString("Location"), l.getString("Location"), String.format("{\"$switch\":\n              {\"branches\": [\n%s\n              ],\n              \"default\": \"undefinedValue\"}}", l.getList("Breakpoint", Document.class).stream().map(
				b -> String.format("                {\"case\": {\"$%s\": [\"$%s\", %s]}, \"then\": \"%s\"}", b.getString("Operator"), classType, b.get("Threshold"), b.getString("Name"))).collect(Collectors.joining(",\n"))))).collect(Collectors.joining(",\n")));
	}
}
