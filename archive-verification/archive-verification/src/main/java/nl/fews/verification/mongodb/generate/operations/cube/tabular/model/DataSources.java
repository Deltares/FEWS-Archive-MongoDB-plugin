package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.shared.crypto.Crypto;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class DataSources implements IModel {
	private final Document template;

	public DataSources(Document template){
		this.template = template;
	}

	/**
	 * Generates the data sources for the template document.
	 * Retrieves the database connection string from the settings.
	 * Parses the database connection string and creates a map of key-value pairs.
	 * Retrieves the necessary options and credentials from the template document.
	 * Updates the options with the corresponding values from the database connection string.
	 * Updates the credentials with the username and password from the database connection string.
	 */
	@Override
	public void generate() {
		String database = Settings.get("databaseConnectionString");
		String username = Settings.get("databaseConnectionUsername");
		String password = Settings.get("databaseConnectionAesPassword");
		Map<String,String> db = Arrays.stream(database.split(";")).filter(s -> s.contains("=")).map(s -> s.split("=")).collect(Collectors.toMap(s -> s[0], s -> s[1]));

		Document options = template.get("model", Document.class).getList("dataSources", Document.class).get(0).get("connectionDetails", Document.class).get("address", Document.class).get("options", Document.class);
		options.append("driver", db.get("driver").replace("{", "").replace("}", ""));
		options.append("server", db.get("server"));
		options.append("port", db.get("port"));

		Document credential = template.get("model", Document.class).getList("dataSources", Document.class).get(0).get("credential", Document.class);
		credential.append("path", database);
		credential.append("Username", username);
		credential.append("Password", Crypto.decrypt(password));
	}
}
