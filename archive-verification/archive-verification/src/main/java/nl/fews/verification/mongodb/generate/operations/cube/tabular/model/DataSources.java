package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.shared.crypto.Crypto;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class DataSources implements IModel {
	private final Document template;

	public DataSources(Document template){
		this.template = template;
	}

	@Override
	public void generate() {
		var acquisitionType = Settings.get("acquisitionType", String.class);

		var dataSource = template.get("model", Document.class).getList("dataSources", Document.class).stream().filter(t -> t.getString("name").equals(acquisitionType)).findFirst().orElse(null);
		if (acquisitionType.equals("mongodb")) {
			var database = Settings.get("databaseConnectionString", String.class);
			var username = Settings.get("fewsArchiveDbUsername", String.class);
			var password = Settings.get("fewsArchiveDbAesPassword", String.class);
			var db = Arrays.stream(database.split(";")).filter(s -> s.contains("=")).map(s -> s.split("=")).collect(Collectors.toMap(s -> s[0], s -> s[1]));

			var options = dataSource.get("connectionDetails", Document.class).get("address", Document.class).get("options", Document.class);
			options.append("driver", db.get("driver").replace("{", "").replace("}", ""));
			options.append("server", db.get("server"));
			options.append("port", db.get("port"));

			var credential = dataSource.get("credential", Document.class);
			credential.append("path", database);
			credential.append("Username", username);
			credential.append("Password", Crypto.decrypt(password));
		}
		else if (acquisitionType.equals("csv")) {
			var csvPath = Settings.get("csvPath", String.class);

			var address = dataSource.get("connectionDetails", Document.class).get("address", Document.class);
			address.append("path", csvPath);

			var credential = dataSource.get("credential", Document.class);
			credential.append("path", csvPath);
		}
		else {
			throw new IllegalArgumentException(acquisitionType);
		}
		template.get("model", Document.class).append("dataSources", List.of(dataSource));
	}
}
