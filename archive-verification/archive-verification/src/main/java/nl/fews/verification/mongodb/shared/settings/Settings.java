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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"unchecked", "unused"})
public final class Settings {

	private static final Logger logger = LoggerFactory.getLogger(Settings.class);

	private static final Map<String, Object> map = new HashMap<>();

	private Settings(){}

	static{
		try {
			String mongoSettingsDbConnection = getMongoDbConnection(System.getenv("FEWS_VERIFICATION_DB_CONNECTION"), System.getenv("FEWS_VERIFICATION_DB_USERNAME"), System.getenv("FEWS_VERIFICATION_DB_AES_PASSWORD"));
			Settings.put("mongoSettingsDbConnection", mongoSettingsDbConnection);
			Settings.put("settingsDb", new ConnectionString(mongoSettingsDbConnection).getDatabase());
			getSettings(mongoSettingsDbConnection, InetAddress.getLocalHost().getHostName().toUpperCase());
			logger.info(String.format("***SETTINGS DB CONNECTED***: [%s]", mongoSettingsDbConnection.replaceAll("//(.+):(.+)@", "//$1:********@")));

			String mongoVerificationDbConnection = getMongoDbConnection(Settings.get("fewsVerificationDbConnection"), Settings.get("fewsVerificationDbUsername"), Settings.get("fewsVerificationDbAesPassword"));
			Settings.put("mongoVerificationDbConnection", mongoVerificationDbConnection);
			Settings.put("verificationDb", new ConnectionString(mongoVerificationDbConnection).getDatabase());
			logger.info(String.format("***VERIFICATION DB CONNECTED***: [%s]", mongoVerificationDbConnection.replaceAll("//(.+):(.+)@", "//$1:********@")));

			String mongoArchiveDbConnection = getMongoDbConnection(Settings.get("fewsArchiveDbConnection"), Settings.get("fewsArchiveDbUsername"), Settings.get("fewsArchiveDbAesPassword"));
			Settings.put("mongoArchiveDbConnection", mongoArchiveDbConnection);
			Settings.put("archiveDb", new ConnectionString(mongoArchiveDbConnection).getDatabase());
			logger.info(String.format("***ARCHIVE DB CONNECTED***: [%s]", mongoArchiveDbConnection.replaceAll("//(.+):(.+)@", "//$1:********@")));
		}
		catch (Exception ex){
			logger.error(ex.getMessage(), ex);
			throw new RuntimeException(ex);
		}
	}

	private static void getSettings(String mongoDbConnection, String hostName){
		String settingsType = System.getenv("FEWS_VERIFICATION_SETTINGS_TYPE");

		try (MongoClient db = MongoClients.create(mongoDbConnection)) {

			Document settings = db.getDatabase(Settings.get("settingsDb")).getCollection("configuration.Settings").find(new Document("environment", settingsType)).limit(1).projection(new Document("_id", 0)).first();
			if(settings != null)
				logger.info(String.format("Using database settings [environment] for [%s].", settingsType));

			if(settings == null) {
				settings = db.getDatabase(Settings.get("settingsDb")).getCollection("configuration.Settings").find(new Document("environment", hostName)).limit(1).projection(new Document("_id", 0)).first();
				if(settings != null)
					logger.info(String.format("Using database settings [environment] for [%s].", hostName));
			}

			if(settings == null) {
				settings = db.getDatabase(Settings.get("settingsDb")).getCollection("configuration.Settings").find(new Document("environment", "Default")).limit(1).projection(new Document("_id", 0)).first();
				if(settings != null)
					logger.info(String.format("Using database settings [environment] for [%s].", "Default"));
			}

			if(settings == null) {
				throw new RuntimeException(String.format("[%s], [%s], [Default] settings not found on [%s]", settingsType, hostName, mongoDbConnection.replaceAll("^.*@", "")));
			}

			settings.forEach(Settings::put);
		}
	}

	private static String getMongoDbConnection(String dbConnection, String dbUsername, String dbAesPassword){
		String userDnsDomain = System.getenv("USERDNSDOMAIN");
		var exceptions = new ArrayList<Exception>();

		if (dbConnection == null || dbConnection.isEmpty()) {
			throw new RuntimeException("[DB_CONNECTION] environment variable not found.");
		}

		try {
			return validMongoDbConnection(dbConnection);
		}
		catch (Exception ex){
			exceptions.add(ex);
		}

		if (dbUsername != null && !dbUsername.isEmpty() && dbAesPassword != null && !dbAesPassword.isEmpty()){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(dbConnection);
				return validMongoDbConnection(m.find() ? m.replaceFirst(String.format("$1%s:%s@$2", dbUsername.replace("$", "\\$"), Crypto.decrypt(dbAesPassword).replace("$", "\\$"))) : dbConnection);
			}
			catch (Exception ex){
				exceptions.add(ex);
			}
		}

		if (!dbConnection.contains("@")){
			try{
				var m = Pattern.compile("^(.+://)(.+)$").matcher(dbConnection);
			 	return validMongoDbConnection(m.find() ? m.replaceFirst(String.format("$1%s%%40%s@$2", System.getProperty("user.name").replace("$", "\\$"), userDnsDomain.replace("$", "\\$"))) : dbConnection);
			}
			catch (Exception ex){
				exceptions.add(ex);
			}
		}

		try {
			var m = Pattern.compile("^(.+:.+:)(.+)(@.+)$").matcher(dbConnection);
			return validMongoDbConnection(m.find() ? m.replaceFirst(String.format("$1%s$3", Crypto.decrypt(m.group(2)).replace("$", "\\$"))) : dbConnection);
		}
		catch (Exception ex) {
			exceptions.add(ex);
		}

		try {
			return validMongoDbConnection(Crypto.decrypt(dbConnection));
		}
		catch (Exception ex){
			exceptions.add(ex);
		}
		logger.error(String.format("Connection string [%s] is invalid.", dbConnection), exceptions);
		throw new RuntimeException(String.format("Connection string [%s] is invalid.\n\n%s", dbConnection, exceptions.stream().map(Throwable::toString).collect(Collectors.joining("\n\n"))));
	}

	private static String validMongoDbConnection(String mongoDbConnection){
		if(new ConnectionString(mongoDbConnection).getHosts().isEmpty()){
			throw new RuntimeException(String.format("Connection string [%s] is invalid.", mongoDbConnection));
		}
		try(var x = MongoClients.create(mongoDbConnection)){
			x.listDatabaseNames().first();
		}
		return mongoDbConnection;
	}

	public static <T> void put(String key, T value) {
		map.put(key, value);
	}

	@SuppressWarnings("unusedParameter")
	public static <T> T get(String key, Class<T> t) {
		return (T)map.get(key);
	}

	public static <T> T get(String key) {
		return (T)map.get(key);
	}

	public static String toJsonString(int indentFactor){
		return new JSONObject(map.entrySet().stream().filter(s -> !s.getKey().toLowerCase().contains("password")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).toString(indentFactor);
	}

	public static JSONObject fromJsonString(String json){
		return new JSONObject(json);
	}
}
