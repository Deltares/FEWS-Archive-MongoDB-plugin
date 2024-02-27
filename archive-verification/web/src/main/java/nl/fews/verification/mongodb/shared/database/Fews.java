package nl.fews.verification.mongodb.shared.database;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import nl.fews.verification.mongodb.generate.shared.parser.Parser;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class Fews {

	private Fews(){}

	/**
	 * Retrieves a Document object containing locations from the REST API by performing various transformations and mappings.
	 *
	 * @return a Document object containing the locations
	 * @throws RuntimeException if an error occurs during the retrieval and transformation process
	 */
	public static Document getLocations(){
		try {
			return getPiJson("locations", "locations", "locations", "locationId");
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves a Document object containing parameters from a specified JSON document by performing various transformations and mappings.
	 *
	 * @return a Document object containing the parameters
	 * @throws RuntimeException if an error occurs during the retrieval and transformation process
	 */
	public static Document getParameters(){
		try {
			return getPiJson("parameters", "parameters", "timeSeriesParameters", "id");
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves the qualifiers from a Document object.
	 *
	 * @return a Document object containing the qualifiers
	 * @throws RuntimeException if an error occurs during the retrieval and transformation process
	 */
	public static Document getQualifiers(){
		try {
			return getPiXml("qualifiers", "qualifiers", "qualifier", "id");
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves a Document object from the given JSON string by performing various transformations and mappings.
	 *
	 * @param query    the query to fetch data from the REST API
	 * @param root     the root element of the Document
	 * @param key      the key used to retrieve the list of elements in the Document
	 * @param itemKey  the key used to map each element in the Document
	 * @return a Document object containing the parsed and transformed data
	 */
	private static Document getPiJson(String query, String root, String key, String itemKey){
		try {
			URI fewsRestApiUri = new URI(String.format("%s/%s?documentFormat=PI_JSON&showAttributes=true", Settings.get("fewsRestApiUri"), query));
			HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(fewsRestApiUri).build(), HttpResponse.BodyHandlers.ofString());
			return getDocument(response.body(), root, key, itemKey);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves a Document object from the given JSON string by performing various transformations and mappings.
	 *
	 * @param query    the query string for the REST API
	 * @param root     the root element of the Document
	 * @param key      the key used to retrieve the list of elements in the Document
	 * @param itemKey  the key used to map each element in the Document
	 * @return a Document object containing the parsed and transformed data
	 * @throws RuntimeException if an error occurs during the retrieval and transformation process
	 */
	private static Document getPiXml(String query, String root, String key, String itemKey){
		try{
			URI fewsRestApiUri = new URI(String.format("%s/%s?documentFormat=PI_XML&showAttributes=true", Settings.get("fewsRestApiUri"), query));
			HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(fewsRestApiUri).build(), HttpResponse.BodyHandlers.ofString());
			return getDocument(new XmlMapper().readTree(response.body()).toString(), root, key, itemKey);
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves a Document object from the given JSON string by performing various transformations and mappings.
	 *
	 * @param json     the JSON string to parse
	 * @param root     the root element of the Document
	 * @param key      the key used to retrieve the list of elements in the Document
	 * @param itemKey  the key used to map each element in the Document
	 * @return a Document object containing the parsed and transformed data
	 */
	private static Document getDocument(String json, String root, String key, String itemKey){
		return new Document(root, Document.parse(json).getList(key, Document.class).stream().collect(
			Collectors.toMap(x -> x.getString(itemKey), x -> x.entrySet().stream().filter(y -> !y.getKey().equals(itemKey)).collect(
				Collectors.toMap(Map.Entry::getKey, y -> !y.getKey().equals("attributes") ? Parser.parseFloat(y.getValue()) : x.getList(y.getKey(), Document.class).stream().collect(
					Collectors.toMap(z -> z.getString("id"), z -> z.entrySet().stream().filter(a -> !a.getKey().equals("id")).collect(
						Collectors.toMap(Map.Entry::getKey, a -> Parser.parseFloat(a.getValue()))), (m, n) -> m, TreeMap::new)))), (k, v) -> k, TreeMap::new))).append("lastUpdated", new Date());
	}
}
