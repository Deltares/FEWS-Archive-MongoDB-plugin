package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Class implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Class(String study){
		this.study = study;
	}

	/**
	 * The `execute` method is designed to create output PowerQuery documents for the categories "ForecastClass", "ObservedClass", and "ContingencyMatrixClass". It operates by fetching necessary data from MongoDB, performing certain transformations on the retrieved data as needed, and subsequently storing the final result back into MongoDB.
	 *
	 * Here's a step-by-step rundown of the method’s execution process:
	 * 1. Retrieves a PowerQuery template from the `Verification` database in MongoDB. It fetches the document from the `template.PowerQuery` collection where the `Name` field matches "Class". This template is then converted into a string representation for further processing.
	 * 2. Fetches the database connection string from the application settings.
	 * 3. Updates the template string by replacing the placeholders, `{study}` and `{database}`, with the given `study` name and the database connection string, respectively.
	 * 4. Splits the updated template string into a list, where each line from the original template becomes a separate item in the list.
	 * 5. Insert three new documents into the `Verification` database in MongoDB, specifically into the `output.PowerQuery` collection. Each of these documents represents one of the following: "ForecastClass", "ObservedClass", or "ContingencyMatrixClass". The `Expression` field for each document is filled with the list derived from the final template string.
`	 *
	 * Note: This method heavily relies on MongoDB operations for data retrieval and inserting, as well as string manipulations. The majority of the computational work involves replacing placeholders with specific values in the template string and using Java 8 Streams to process the data.
	 */
	@Override
	public void execute(){
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Class")).getList("Template", String.class));

		String database = Settings.get("databaseConnectionString");
		String query = String.format("SELECT * FROM %s.`%s`", Settings.get("verificationDb"), String.format("%s_Class", study));

		template = template.replace("{database}", database);
		template = template.replace("{query}", query);

		List<String> _class = Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList());
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastClass").append("Month", "").append("Expression", _class));
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ObservedClass").append("Month", "").append("Expression", _class));
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ContingencyMatrixClass").append("Month", "").append("Expression", _class));
	}

	/**
	 * Retrieves an array of predecessor names for the current class.
	 *
	 * @return an array of predecessor names
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
