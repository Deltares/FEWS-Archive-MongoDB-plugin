package nl.fews.verification.mongodb.shared.settings;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import nl.fews.verification.mongodb.shared.crypto.Crypto;

import org.bson.Document;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unchecked", "unused"})
public final class Settings {

	/**
	 *
	 */
	private static final Logger logger = LoggerFactory.getLogger(Settings.class);

	/**
	 * settings collection
	 */
	private static final Map<String, Object> map = new HashMap<>();

	/**
	 * static class
	 */
	private Settings(){}

	static{
		try {
			String mongoArchiveDbConnection = getMongoArchiveDbConnection();
			getSettings(mongoArchiveDbConnection, InetAddress.getLocalHost().getHostName().toUpperCase());
		}
		catch (Exception ex){
			logger.error(ex.getMessage(), ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves and sets the settings for the given mongoArchiveDbConnection and hostName.
	 * If the settings for the given hostName are not found in the database, it will fallback
	 * to the "Default" settings.
	 *
	 * @param mongoArchiveDbConnection the connection string for the MongoDB archive database
	 * @param hostName the name of the host environment
	 */
	private static void getSettings(String mongoArchiveDbConnection, String hostName){
		Settings.put("mongoArchiveDbConnection", mongoArchiveDbConnection);
		try (MongoClient db = MongoClients.create(mongoArchiveDbConnection)) {
			Document settings = db.getDatabase("Verification").getCollection("configuration.Settings").find(new Document("environment", hostName)).limit(1).projection(new Document("_id", 0)).first();
			if(settings == null) {
				logger.warn(String.format("[%s] not found in database settings.  Using [Default] instead.", hostName));
				settings = db.getDatabase("Verification").getCollection("configuration.Settings").find(new Document("environment", "Default")).limit(1).projection(new Document("_id", 0)).first();
			}
			if(settings == null) {
				throw new RuntimeException(String.format("[Default] settings not found on [%s]", mongoArchiveDbConnection.replaceAll("^.*@", "")));
			}
			settings.forEach(Settings::put);
		}
	}

	/**
	 * Retrieves the MongoDB archive database connection string.
	 *
	 * @return The MongoDB archive database connection string.
	 * @throws RuntimeException If the [FEWS_ARCHIVE_DB_CONNECTION] environment variable is not found or the connection string is invalid.
	 */
	private static String getMongoArchiveDbConnection(){
		String mongoArchiveDbConnection = System.getenv("FEWS_ARCHIVE_DB_CONNECTION");
		String mongoArchiveDbUsername = System.getenv("FEWS_ARCHIVE_DB_USERNAME");
		String mongoArchiveDbAesPassword = System.getenv("FEWS_ARCHIVE_DB_AES_PASSWORD");
		String userDnsDomain = System.getenv("USERDNSDOMAIN");

		if (mongoArchiveDbConnection == null || mongoArchiveDbConnection.isEmpty()) {
			throw new RuntimeException("[FEWS_ARCHIVE_DB_CONNECTION] environment variable not found.");
		}

		try {
			return validMongoArchiveDbConnection(mongoArchiveDbConnection);
		}
		catch (Exception ex){
			//TRY NEXT
		}

		if (mongoArchiveDbUsername != null && !mongoArchiveDbUsername.isEmpty() && mongoArchiveDbAesPassword != null && !mongoArchiveDbAesPassword.isEmpty()){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(mongoArchiveDbConnection);
				return validMongoArchiveDbConnection(m.find() ? m.replaceFirst(String.format("$1%s:%s@$2", mongoArchiveDbUsername, Crypto.decrypt(mongoArchiveDbAesPassword))) : mongoArchiveDbConnection);
			}
			catch (Exception ex){
				//TRY NEXT
			}
		}

		if (!mongoArchiveDbConnection.contains("@")){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(mongoArchiveDbConnection);
			 	return validMongoArchiveDbConnection(m.find() ? m.replaceFirst(String.format("$1%s%%40%s@$2", System.getProperty("user.name"), userDnsDomain)) : mongoArchiveDbConnection);
			}
			catch (Exception ex){
				//TRY NEXT
			}
		}

		try {
			var m = Pattern.compile("^(.+:.+:)(.+)(@.+)$").matcher(mongoArchiveDbConnection);
			return validMongoArchiveDbConnection(m.find() ? m.replaceFirst(String.format("$1%s$3", Crypto.decrypt(m.group(2)))) : mongoArchiveDbConnection);
		}
		catch (Exception ex) {
			//TRY NEXT
		}

		try {
			return validMongoArchiveDbConnection(Crypto.decrypt(mongoArchiveDbConnection));
		}
		catch (Exception ex){
			//TRY NEXT
		}

		throw new RuntimeException(String.format("Connection string [%s] is invalid.", mongoArchiveDbConnection));
	}

	/**
	 * Validates the given MongoDB archive database connection string.
	 *
	 * @param mongoArchiveDbConnection the connection string for the MongoDB archive database
	 * @return the validated connection string if it is valid
	 * @throws RuntimeException if the connection string is invalid
	 */
	private static String validMongoArchiveDbConnection(String mongoArchiveDbConnection){
		if(new ConnectionString(mongoArchiveDbConnection).getHosts().isEmpty()){
			throw new RuntimeException(String.format("Connection string [%s] is invalid.", mongoArchiveDbConnection));
		}

		try(var x = MongoClients.create(mongoArchiveDbConnection)){
			x.listDatabaseNames().first();
		}
		return mongoArchiveDbConnection;
	}

	/**
	 *
	 * @param key key
	 * @param value object value
	 */
	public static <T> void put(String key, T value) {
		map.put(key, value);
	}

	/**
	 *
	 * @param key key
	 * @param t type based on t.class
	 * @return typed value based on t
	 */
	@SuppressWarnings("unusedParameter")
	public static <T> T get(String key, Class<T> t) {
		return (T)map.get(key);
	}

	/**
	 *
	 * @param key key
	 * @return typed value
	 */
	public static <T> T get(String key) {
		return (T)map.get(key);
	}

	/**
	 * @param indentFactor indentFactor
	 * @return String
	 */
	public static String toJsonString(int indentFactor){
		return new JSONObject(map.entrySet().stream().filter(s -> !s.getKey().toLowerCase().contains("password")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).toString(indentFactor);
	}

	public static JSONObject fromJsonString(String json){
		return new JSONObject(json);
	}
}
