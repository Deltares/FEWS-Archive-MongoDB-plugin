package nl.fews.verification.mongodb.shared.database;

import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Index {

	private Index(){}

	public static final Map<String, Document[]> collectionIndex = Map.ofEntries(
			Map.entry("Class", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("Forecast", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("LocationAttributes", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("Normal", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("Observed", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("Seasonality", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("Study", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("configuration.Description", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("configuration.Settings", new Document[]{
					new Document(Stream.of("environment").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("dimension.IsOriginalForecast", new Document[]{
					new Document(Stream.of("isOriginalForecast").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("dimension.IsOriginalObserved", new Document[]{
					new Document(Stream.of("isOriginalObserved").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("dimension.Measure", new Document[]{
					new Document(Stream.of("measureId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("fews.Locations", new Document[]{
					new Document(Stream.of("lastUpdated").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("fews.Parameters", new Document[]{
					new Document(Stream.of("lastUpdated").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("fews.Qualifiers", new Document[]{
					new Document(Stream.of("lastUpdated").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
				Map.entry("output.PowerQuery", new Document[]{
					new Document(Stream.of("Study", "Name", "Month").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
					new Document(Stream.of("Study", "Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
					new Document(Stream.of("Study").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
			}),
			Map.entry("template.Cube", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("template.DrdlYaml", new Document[]{
					new Document(Stream.of("Type", "Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			}),
			Map.entry("template.PowerQuery", new Document[]{
					new Document(Stream.of("Name").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1)
			})
	);
}
