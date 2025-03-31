package nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Common.deduplicateTime;
import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Common.getValueClass;

public final class Forecast {

	private Forecast(){}

    public static List<Document> getForecastData(Document studyDocument, YearMonth month, String locationId){
		List<Document> forecasts = studyDocument.getList("Forecasts", String.class).stream().map(n -> Mongo.findOne("Forecast", new Document("Name", n))).toList();
		var forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
		var forecastClass = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));

		var results = new ArrayList<Document>();
		forecasts.forEach(filter -> {
			var forecastName = filter.getString("ForecastName");
			filter.getList("Filters", Document.class).forEach(f -> {
				var locationMap = f.get("LocationMap", Document.class).isEmpty() ? "$locationId" : Document.parse(Conversion.getLocationMap(f.get("LocationMap", Document.class)));
				var documents = new ArrayList<Document>();
				var pipeline = List.of(
					new Document("$match", f.get("Filter", Document.class).append("locationId", locationId).append(forecastTime, new Document("$gte", Conversion.getYearMonthDate(month)).append("$lt", Conversion.getYearMonthDate(month.plusMonths(1))))),
					new Document("$addFields",
						new Document("dispatchTime", "$runInfo.dispatchTime")
							.append("forecastName", forecastName)
							.append("forecastId", new Document("$trim", new Document("input", new Document("$concat", List.of(forecastName, "_", "$ensembleId", "_", "$ensembleMemberId"))).append("chars", "_")))
							.append("location", locationMap)
							.append("ensemble", "$ensembleId")
							.append("ensembleMember", "$ensembleMemberId")
							.append("timeStepMinutes", "$metaData.timeStepMinutes")
							.append("forecastTime", new Document("$dateAdd", new Document("startDate", new Document("$dateTrunc", new Document("date", "$localForecastTime").append("unit", "day"))).append("unit", "minute").append("amount", new Document("$toInt", new Document("$multiply", List.of(new Document("$floor", new Document("$divide", List.of(new Document("$add", List.of(new Document("$multiply", List.of(new Document("$hour", "$localForecastTime"), 60)), new Document("$minute", "$localForecastTime"))), "$metaData.timeStepMinutes"))), "$metaData.timeStepMinutes"))))))
							.append("forecastDate", new Document("$dateTrunc", new Document("date", new Document("$dateAdd", new Document("startDate", new Document("$dateTrunc", new Document("date", "$localForecastTime").append("unit", "day"))).append("unit", "minute").append("amount", new Document("$toInt", new Document("$multiply", List.of(new Document("$floor", new Document("$divide", List.of(new Document("$add", List.of(new Document("$multiply", List.of(new Document("$hour", "$localForecastTime"), 60)), new Document("$minute", "$localForecastTime"))), "$metaData.timeStepMinutes"))), "$metaData.timeStepMinutes")))))).append("unit", "day")))
							.append("forecastMinute", new Document("$toInt", new Document("$multiply", List.of(new Document("$floor", new Document("$divide", List.of(new Document("$add", List.of(new Document("$multiply", List.of(new Document("$hour", "$localForecastTime"), 60)), new Document("$minute", "$localForecastTime"))), "$metaData.timeStepMinutes"))), "$metaData.timeStepMinutes"))))),
					new Document("$project", new Document("_id", 0).append("dispatchTime", 1).append("forecastName", 1).append("forecastId", 1).append("location", 1).append("ensemble", 1).append("ensembleMember", 1).append("forecastTime", 1).append("forecastDate", 1).append("forecastMinute", 1).append("timeStepMinutes", 1).append(String.format("timeseries.%s", eventTime), 1).append(String.format("timeseries.%s", eventValue), 1)),
					new Document("$sort", new Document("forecastTime", 1).append("dispatchTime", -1))
				);
				Mongo.aggregate(Settings.get("archiveDb", String.class), filter.getString("Collection"), pipeline).forEach(r -> {
					r.append("timeseries", fFillForecastTimeseries(r, eventTime, eventValue));
					documents.add(r);
				});
				results.addAll(unwindForecastTimeseries(deduplicateTime(fFillForecast(documents), "forecastTime"), forecastClass));
			});
		});
		return results;
	}
	
	private static List<Document> fFillForecastTimeseries(Document forecast, String eventTime, String eventValue){
		var timeseries = new ArrayList<Document>();
		var ft = forecast.getDate("forecastTime").toInstant();
		var m = forecast.getInteger("timeStepMinutes");
		var v = Double.NaN;
		var l = 0;
		var i = 0;
		var t = forecast.getList("timeseries", Document.class);
		for (Instant d = t.get(0).getDate(eventTime).toInstant(); d.compareTo(t.get(t.size()-1).getDate(eventTime).toInstant()) <= 0; d = d.plusSeconds(m * 60L)) {
			var et = t.get(i).getDate(eventTime).toInstant();
			var ev = t.get(i).getDouble(eventValue);
			if (d.equals(et)){
				if (ev != null && Double.isFinite(ev)) {
					v = ev;
					l = 0;
				}
				i++;
			}
			l += m;
			if (d.compareTo(ft) >= 0) {
				if (l <= 24 * 60)
					timeseries.add(new Document("eventTime", Date.from(d)).append("forecast", v).append("isOriginalForecast", l == m));
				else
					timeseries.add(new Document("eventTime", Date.from(d)).append("forecast", null).append("isOriginalForecast", true));
			}
		}
		return timeseries;
	}

	private static List<Document> unwindForecastTimeseries(List<Document> forecasts, Document forecastClass){
		var unwound = new ArrayList<Document>();
		for (var d: forecasts) {
			for (var t: d.getList("timeseries", Document.class)) {
				var i = t.getDate("eventTime").toInstant();
				unwound.add(
					new Document("forecastName", d.getString("forecastName"))
					.append("forecastId", d.getString("forecastId"))
					.append("location", d.getString("location"))
					.append("ensemble", d.getString("ensemble"))
					.append("ensembleMember", d.getString("ensembleMember"))
					.append("forecastTime", d.getDate("forecastTime"))
					.append("forecastDate", d.getDate("forecastDate"))
					.append("forecastMinute", d.getInteger("forecastMinute"))
					.append("isOriginalForecast", t.getBoolean("isOriginalForecast"))
					.append("eventTime", t.getDate("eventTime"))
					.append("eventDate", Date.from(i.atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()))
					.append("eventMinute", i.atZone(ZoneOffset.UTC).getHour() * 60 + i.atZone(ZoneOffset.UTC).getMinute())
					.append("leadTime", (t.getDate("eventTime").getTime() - d.getDate("forecastTime").getTime()) / (60 * 1000))
					.append("forecast", t.getDouble("forecast"))
					.append("forecastClass", getValueClass(forecastClass, t.getDouble("forecast"), d.getString("location"))));
			}
		}
		return unwound;
	}
	
	private static List<Document> fFillForecast(List<Document> forecasts) {
		var filled = new ArrayList<Document>();
		Document curr = null;
		for (var partition: forecasts.stream().collect(Collectors.groupingBy(d -> d.getString("location"))).entrySet()){
			for (var next : partition.getValue().stream().sorted(Comparator.comparing(d -> d.getDate("forecastTime").getTime())).toList()) {
				if (curr != null) {
					filled.add(curr);
					var m = curr.getInteger("timeStepMinutes");
					for (
							Instant i = curr.getDate("forecastTime").toInstant().plusSeconds(m * 60L);
							i.compareTo(next.getDate("forecastTime").toInstant()) < 0 && i.compareTo(curr.getDate("forecastTime").toInstant().plus(24, ChronoUnit.HOURS)) <= 0;
							i = i.plusSeconds(m * 60L)) {
						var copy = Document.parse(curr.toJson());
						copy.append("forecastTime", Date.from(i));
						copy.append("forecastDate", Date.from(i.atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()));
						copy.append("forecastMinute", i.atZone(ZoneOffset.UTC).getHour() * 60 + i.atZone(ZoneOffset.UTC).getMinute());
						Instant fi = i;
						copy.append("timeseries", copy.getList("timeseries", Document.class).stream().filter(t -> t.getDate("eventTime").toInstant().compareTo(fi) >= 0).map(t -> t.append("isOriginalForecast", false)).toList());
						filled.add(copy);
					}
				}
				curr = next;
			}
			filled.add(curr);
		}
		return filled;
	}
}
