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
			String mongoVerificationDbConnection = getMongoVerificationDbConnection();
			getSettings(mongoVerificationDbConnection, InetAddress.getLocalHost().getHostName().toUpperCase());
		}
		catch (Exception ex){
			logger.error(ex.getMessage(), ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Retrieves and sets the settings for the given mongoVerificationDbConnection and hostName.
	 * If the settings for the given hostName are not found in the database, it will fallback
	 * to the "Default" settings.
	 *
	 * @param mongoDbConnection the connection string for the MongoDB database
	 * @param hostName the name of the host environment
	 */
	private static void getSettings(String mongoDbConnection, String hostName){
		Settings.put("mongoVerificationDbConnection", mongoDbConnection);
		try (MongoClient db = MongoClients.create(mongoDbConnection)) {
			Document settings = db.getDatabase("Verification").getCollection("configuration.Settings").find(new Document("environment", hostName)).limit(1).projection(new Document("_id", 0)).first();
			if(settings == null) {
				logger.warn(String.format("[%s] not found in database settings.  Using [Default] instead.", hostName));
				settings = db.getDatabase("Verification").getCollection("configuration.Settings").find(new Document("environment", "Default")).limit(1).projection(new Document("_id", 0)).first();
			}
			if(settings == null) {
				throw new RuntimeException(String.format("[Default] settings not found on [%s]", mongoDbConnection.replaceAll("^.*@", "")));
			}
			settings.forEach(Settings::put);
		}
	}

	/**
	 * Retrieves the MongoDB database connection string.
	 *
	 * @return The MongoDB database connection string.
	 * @throws RuntimeException If the [FEWS_VERIFICATION_DB_CONNECTION] environment variable is not found or the connection string is invalid.
	 */
	private static String getMongoVerificationDbConnection(){
		String mongoVerificationDbConnection = System.getenv("FEWS_VERIFICATION_DB_CONNECTION");
		String mongoVerificationDbUsername = System.getenv("FEWS_VERIFICATION_DB_USERNAME");
		String mongoVerificationDbAesPassword = System.getenv("FEWS_VERIFICATION_DB_AES_PASSWORD");
		String userDnsDomain = System.getenv("USERDNSDOMAIN");

		if (mongoVerificationDbConnection == null || mongoVerificationDbConnection.isEmpty()) {
			throw new RuntimeException("[FEWS_VERIFICATION_DB_CONNECTION] environment variable not found.");
		}

		try {
			return validMongoVerificationDbConnection(mongoVerificationDbConnection);
		}
		catch (Exception ex){
			//TRY NEXT
		}

		if (mongoVerificationDbUsername != null && !mongoVerificationDbUsername.isEmpty() && mongoVerificationDbAesPassword != null && !mongoVerificationDbAesPassword.isEmpty()){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(mongoVerificationDbConnection);
				return validMongoVerificationDbConnection(m.find() ? m.replaceFirst(String.format("$1%s:%s@$2", mongoVerificationDbUsername, Crypto.decrypt(mongoVerificationDbAesPassword))) : mongoVerificationDbConnection);
			}
			catch (Exception ex){
				//TRY NEXT
			}
		}

		if (!mongoVerificationDbConnection.contains("@")){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(mongoVerificationDbConnection);
			 	return validMongoVerificationDbConnection(m.find() ? m.replaceFirst(String.format("$1%s%%40%s@$2", System.getProperty("user.name"), userDnsDomain)) : mongoVerificationDbConnection);
			}
			catch (Exception ex){
				//TRY NEXT
			}
		}

		try {
			var m = Pattern.compile("^(.+:.+:)(.+)(@.+)$").matcher(mongoVerificationDbConnection);
			return validMongoVerificationDbConnection(m.find() ? m.replaceFirst(String.format("$1%s$3", Crypto.decrypt(m.group(2)))) : mongoVerificationDbConnection);
		}
		catch (Exception ex) {
			//TRY NEXT
		}

		try {
			return validMongoVerificationDbConnection(Crypto.decrypt(mongoVerificationDbConnection));
		}
		catch (Exception ex){
			//TRY NEXT
		}

		throw new RuntimeException(String.format("Connection string [%s] is invalid.", mongoVerificationDbConnection));
	}

	/**
	 * Validates the given MongoDB database connection string.
	 *
	 * @param mongoVerificationDbConnection the connection string for the MongoDB database
	 * @return the validated connection string if it is valid
	 * @throws RuntimeException if the connection string is invalid
	 */
	private static String validMongoVerificationDbConnection(String mongoVerificationDbConnection){
		if(new ConnectionString(mongoVerificationDbConnection).getHosts().isEmpty()){
			throw new RuntimeException(String.format("Connection string [%s] is invalid.", mongoVerificationDbConnection));
		}

		try(var x = MongoClients.create(mongoVerificationDbConnection)){
			x.listDatabaseNames().first();
		}
		return mongoVerificationDbConnection;
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
