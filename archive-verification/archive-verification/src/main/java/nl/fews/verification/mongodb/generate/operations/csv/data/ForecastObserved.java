package nl.fews.verification.mongodb.generate.operations.csv.data;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Common.deduplicateTime;
import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Forecast.getForecastData;
import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Normal.getNormalData;
import static nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved.Observed.getObservedData;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var dataPath = Path.of(Settings.get("csvPath", String.class), "data", study, name);
		if (!Files.exists(dataPath))
			IO.createDirectories(dataPath);

		var reprocessCube = Arrays.stream(Settings.get("reprocessCubes", String.class).split(",")).map(String::trim).toList().contains(String.format("Verification_%s", study));
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var forecastStartMonth = studyDocument.getString("ForecastStartMonth");
		var format = Conversion.getMonthDateTimeFormatter();
		var startMonth = YearMonth.parse(forecastStartMonth);
		var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);
		var reprocessStartMonth = reprocessCube ? startMonth : YearMonth.parse(LocalDateTime.now().minusDays(studyDocument.getInteger("ReprocessDays")).format(format), format);

		var months = new ArrayList<YearMonth>();
		for(var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1)) {
			var path = Path.of(dataPath.toString(), String.format("%s_%s_%s.csv.zip", study, name, m.format(format)));
			var exists = Files.exists(path);
			var lastModified = exists ? IO.lastModified(path) : null;
			var filesCurrentForSeconds = Settings.get("filesCurrentForSeconds", Integer.class);
			if (!exists || lastModified.compareTo(new Date().toInstant().minusSeconds(filesCurrentForSeconds)) < 0 && m.compareTo(reprocessStartMonth) >= 0) {
				months.add(m);
			}
		}

		var pool = Executors.newFixedThreadPool(Settings.get("threads", Integer.class));
		try {
			List<Future<Object>> results = pool.invokeAll(months.stream().map(m -> (Callable<Object>) () -> query(m, format, dataPath, name, studyDocument)).toList());
			for (Future<Object> x : results) {
				x.get();
			}
		}
		catch (InterruptedException | ExecutionException ex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(ex);
		}
		finally {
			pool.shutdown();
		}
	}

	private Object query(YearMonth month, DateTimeFormatter format, Path dataPath, String name, Document studyDocument){
		var tempPath = Path.of(dataPath.toString(), String.format("%s_%s_%s.csv.zip.temp", study, name, month.format(format)));
		var commitPath = Path.of(dataPath.toString(), String.format("%s_%s_%s.csv.zip", study, name, month.format(format)));

		try (var zip = new ZipOutputStream(Files.newOutputStream(tempPath)); var writer = new BufferedWriter(new OutputStreamWriter(zip), 128 * 1024))
		{
			zip.putNextEntry(new ZipEntry(commitPath.getFileName().toString().replace(".zip", "")));
			writer.write(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				"forecastName", "forecastId", "location", "ensemble", "ensembleMember", "forecastTime", "forecastDate", "forecastMinute", "isOriginalForecast", "eventTime", "eventDate", "eventMinute", "leadTime", "forecast", "forecastClass", "isOriginalObserved", "observed", "observedClass"));

			for (var locationId: getForecastLocationIds(studyDocument, month)) {

				var forecasts = getForecastData(studyDocument, month, locationId);
				var observed = getObservedData(studyDocument, month, locationId);
				var normal = getNormalData(studyDocument, month, locationId);

				var forecastsCollapsed = collapseForecastLocationTime(forecasts);
				var observedLookup = deduplicateTime(observed, "eventTime").stream().collect(Collectors.groupingBy(f -> f.getString("location"), Collectors.toMap(t -> t.getDate("eventTime"), t -> t)));
				var normalLookup = deduplicateTime(normal, "forecastTime").stream().collect(Collectors.groupingBy(f -> f.getString("location"), Collectors.toMap(t -> t.getDate("forecastTime"), t -> t)));

				forecasts.addAll(forecastsCollapsed.stream().filter(f -> normalLookup.containsKey(f.getString("location")) && normalLookup.get(f.getString("location")).containsKey(f.getDate("forecastTime"))).map(f -> {
					var n = normalLookup.get(f.getString("location")).get(f.getDate("forecastTime"));
					return new Document(f)
						.append("isOriginalForecast", n.getBoolean("isOriginalForecast"))
						.append("forecast", n.getDouble("forecast"))
						.append("forecastClass", n.getString("forecastClass"))
						.append("forecastName", "Normal")
						.append("forecastId", "Normal")
						.append("ensemble", "")
						.append("ensembleMember", "");
				}).toList());

				for (var forecast : forecasts){
					if (observedLookup.containsKey(forecast.getString("location")) && observedLookup.get(forecast.getString("location")).containsKey(forecast.getDate("eventTime"))){
						var n = observedLookup.get(forecast.getString("location")).get(forecast.getDate("eventTime"));
						forecast.append("isOriginalObserved", n.getBoolean("isOriginalObserved")).append("observed", n.getDouble("observed")).append("observedClass", n.getString("observedClass"));
						StringBuilder sb = new StringBuilder(512);
						writer.write(sb.append(forecast.getString("forecastName")).append('\t')
							.append(forecast.getString("forecastId")).append('\t')
							.append(forecast.getString("location")).append('\t')
							.append(forecast.getString("ensemble")).append('\t')
							.append(forecast.getString("ensembleMember")).append('\t')
							.append(forecast.getDate("forecastTime").toInstant()).append('\t')
							.append(forecast.getDate("forecastDate").toInstant()).append('\t')
							.append(forecast.getInteger("forecastMinute")).append('\t')
							.append(forecast.getBoolean("isOriginalForecast")).append('\t')
							.append(forecast.getDate("eventTime").toInstant()).append('\t')
							.append(forecast.getDate("eventDate").toInstant()).append('\t')
							.append(forecast.getInteger("eventMinute")).append('\t')
							.append(forecast.getLong("leadTime")).append('\t')
							.append(forecast.getDouble("forecast")).append('\t')
							.append(forecast.getString("forecastClass")).append('\t')
							.append(forecast.getBoolean("isOriginalObserved")).append('\t')
							.append(forecast.getDouble("observed")).append('\t')
							.append(forecast.getString("observedClass")).append('\n').toString());
					}
				}
			}
			writer.flush();
			zip.closeEntry();
			IO.moveFile(tempPath, commitPath);
		}
		catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		return null;
	}

	private static List<String> getForecastLocationIds(Document studyDocument, YearMonth month){
		var forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));

		return studyDocument.getList("Forecasts", String.class).stream().map(n ->
			Mongo.findOne("Forecast", new Document("Name", n))).flatMap(filter ->
				filter.getList("Filters", Document.class).stream().flatMap(f ->
					StreamSupport.stream(Mongo.distinct(Settings.get("archiveDb", String.class), filter.getString("Collection"), "locationId",
						f.get("Filter", Document.class).append(forecastTime, new Document("$gte", startMonth).append("$lt", endMonth)), String.class).spliterator(), false))).toList();
	}

	private List<Document> collapseForecastLocationTime(List<Document> forecasts){
		var deduplicated = new ArrayList<Document>();
		var seen = new HashSet<String>();
		for (var r : forecasts) {
			var k = String.format("%s|%s|%s", r.getString("location"), r.getDate("forecastTime").toInstant(), r.getDate("eventTime").toInstant());
			if (!seen.contains(k)) {
				seen.add(k);
				deduplicated.add(r);
			}
		}
		return deduplicated;
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
