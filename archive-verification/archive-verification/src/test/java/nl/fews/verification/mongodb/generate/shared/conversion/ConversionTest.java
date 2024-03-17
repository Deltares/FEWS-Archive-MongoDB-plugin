package nl.fews.verification.mongodb.generate.shared.conversion;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConversionTest {

	@Test
	void getSqlType() {
		assertEquals("varchar", Conversion.getSqlType("string"));
        assertEquals("boolean", Conversion.getSqlType("bool"));
        assertEquals("int", Conversion.getSqlType("int"));
        assertEquals("int", Conversion.getSqlType("int64"));
        assertEquals("float", Conversion.getSqlType("float64"));
        assertEquals("decimal", Conversion.getSqlType("bson.Decimal128"));
        assertEquals("unknown", Conversion.getSqlType("unknown"));
	}

	@Test
	void getBsonType() {
		assertEquals("string", Conversion.getBsonType("String"));
		assertEquals("bool", Conversion.getBsonType("Boolean"));
		assertEquals("int", Conversion.getBsonType("Integer"));
		assertEquals("int64", Conversion.getBsonType("Int64"));
		assertEquals("float64", Conversion.getBsonType("Double"));
		assertEquals("bson.Decimal128", Conversion.getBsonType("Decimal"));
		assertEquals("unknown", Conversion.getBsonType("unknown"));
	}

	@Test
	void getCubeType() {
		assertEquals("string", Conversion.getCubeType("String"));
		assertEquals("boolean", Conversion.getCubeType("Boolean"));
		assertEquals("int64", Conversion.getCubeType("Integer"));
		assertEquals("int64", Conversion.getCubeType("Int64"));
		assertEquals("double", Conversion.getCubeType("Double"));
		assertEquals("double", Conversion.getCubeType("Decimal"));
		assertEquals("unknown", Conversion.getCubeType("unknown"));
	}

	@Test
	void getForecastTime() {
		assertEquals("localForecastTime", Conversion.getForecastTime("local"));
		assertEquals("forecastTime", Conversion.getForecastTime("other"));
	}

	@Test
	void getEventTime() {
		assertEquals("lt", Conversion.getEventTime("local"));
		assertEquals("t", Conversion.getEventTime("other"));
	}

	@Test
	void getStartTime() {
		assertEquals("localStartTime", Conversion.getStartTime("local"));
		assertEquals("startTime", Conversion.getStartTime("other"));
	}

	@Test
	void getEndTime() {
		assertEquals("localEndTime", Conversion.getEndTime("local"));
		assertEquals("endTime", Conversion.getEndTime("other"));
	}

	@Test
	void getEventValue() {
		assertEquals("dv", Conversion.getEventValue("display"));
		assertEquals("v", Conversion.getEventValue("other"));
	}

	@Test
	void getSeasonalities() {
		List<Document> seasonalities = List.of(new Document("Name", "Season").append("Breakpoint", List.of(new Document("Operator", "gte").append("Threshold", "06-01").append("Name", "summer"))));
        assertEquals("        \"SeasonSeason\": {\"$switch\": {\"branches\": [\n          {\"case\": {\"$gte\": [\"$monthDay\", \"06-01\"]}, \"then\": \"summer\"}\n          ]}}", Conversion.getSeasonalities(seasonalities));
	}

	@Test
	void getLocationMap() {
		assertEquals("\"$locationId\"", Conversion.getLocationMap(new Document()));
		assertEquals("{\"$switch\":\n          {\"branches\": [\n            {\"case\": {\"$eq\": [\"$locationId\", \"loc1\"]}, \"then\": \"MappedLoc1\"},\n            {\"case\": {\"$eq\": [\"$locationId\", \"loc2\"]}, \"then\": \"MappedLoc2\"}\n          ],\n          \"default\": \"$locationId\"}}", Conversion.getLocationMap(new Document("loc1", "MappedLoc1").append("loc2", "MappedLoc2")));
	}

	@Test
	void getSeasonalityColumns() {
		assertEquals("    - Name: WinterSeason\n      MongoType: string\n      SqlName: WinterSeason\n      SqlType: varchar\n    - Name: SpringSeason\n      MongoType: string\n      SqlName: SpringSeason\n      SqlType: varchar\n    - Name: SummerSeason\n      MongoType: string\n      SqlName: SummerSeason\n      SqlType: varchar\n    - Name: FallSeason\n      MongoType: string\n      SqlName: FallSeason\n      SqlType: varchar", Conversion.getSeasonalityColumns(List.of("Winter", "Spring", "Summer", "Fall")));
	}

	@Test
	void getMonthDateTimeFormatter() {
        assertEquals(DateTimeFormatter.ofPattern("yyyy-MM").toString(), Conversion.getMonthDateTimeFormatter().toString());
	}

	@Test
	void getLocationAttributeTypes() {
		assertEquals(
			new Document("locationId", "String").append("TestAttribute", "String").append("AnotherAttribute", "Double"),
			Conversion.getLocationAttributeTypes(new Document("locations",
				new Document("Location1", new Document("locationId", "Location1").append("attributes", new Document("TestAttribute", new Document("value", "ExampleValue")))).
				append("Location2", new Document("locationId", "Location2").append("attributes", new Document("AnotherAttribute", new Document("value", 1.0))))), new Document("Attributes", List.of("TestAttribute", "AnotherAttribute"))));
	}

	@Test
	void getObservedClass() {
		assertEquals(
			"{\"$switch\":\n          {\"branches\": [\n            {\"case\": {\"$or\": [{\"$eq\": [\"\", \"loc1\"]}, {\"$eq\": [\"$location\", \"loc1\"]}]}, \"then\": {\"$switch\":\n              {\"branches\": [\n                {\"case\": {\"$lte\": [\"$observed\", 1.0]}, \"then\": \"one\"}\n              ],\n              \"default\": \"undefinedValue\"}}}\n          ],\n          \"default\": \"undefinedLocation\"}}",
			Conversion.getObservedClass(new Document("Locations", List.of(new Document("Location", "loc1").append("Breakpoint", List.of(new Document("Operator", "lte").append("Threshold", 1.0).append("Name", "one")))))));
	}

	@Test
	void getForecastClass() {
		assertEquals(
			"{\"$switch\":\n          {\"branches\": [\n            {\"case\": {\"$or\": [{\"$eq\": [\"\", \"loc1\"]}, {\"$eq\": [\"$location\", \"loc1\"]}]}, \"then\": {\"$switch\":\n              {\"branches\": [\n                {\"case\": {\"$lte\": [\"$forecast\", 1.0]}, \"then\": \"one\"}\n              ],\n              \"default\": \"undefinedValue\"}}}\n          ],\n          \"default\": \"undefinedLocation\"}}",
			Conversion.getForecastClass(new Document("Locations", List.of(new Document("Location", "loc1").append("Breakpoint", List.of(new Document("Operator", "lte").append("Threshold", 1.0).append("Name", "one")))))));
	}
}