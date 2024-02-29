package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class IsOriginalForecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public IsOriginalForecast(String study){
		this.study = study;
	}

	/**
	 * Executes the IsOriginalForecast operation.
	 *
	 * This method retrieves a template from the "Verification" collection in the "template.PowerQuery" document
	 * with the name "IsOriginalForecast". It then retrieves the connection string for the database from the
	 * "Settings" class. The template is modified by replacing "{database}" with the retrieved database connection
	 * string. Finally, the modified template is inserted into the "Verification" collection in the
	 * "output.PowerQuery" document, along with the provided study, name, month and expression.
	 *
	 * @see IExecute
	 * @see IPredecessor
	 */
	@Override
	public void execute(){
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "IsOriginalForecast")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");

		template = template.replace("{database}", database);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "IsOriginalForecast").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
	}

	/**
	 * Returns an array of strings representing the predecessors of the current object.
	 *
	 * @return an array of strings representing the predecessors
	 *
	 * @see IPredecessor
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
