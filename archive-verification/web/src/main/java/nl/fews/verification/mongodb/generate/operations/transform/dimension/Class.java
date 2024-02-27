package nl.fews.verification.mongodb.generate.operations.transform.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class Class implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Class(String study){
		this.study = study;
	}

	/**
	 * Executes the method to generate and write a DRDL YAML file.
	 *
	 * The method retrieves a template DRDL YAML file from the database,
	 * replaces a placeholder with the given study, and writes the modified
	 * template to a file in the specified directory.
	 */
	@Override
	public void execute(){
		String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", "Verification").append("Type", "Dimension").append("Name", "Class")).getList("Template", String.class));
		template = template.replace("{study}", study);
		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_Class.drdl.yml", study)).toString(), template);
	}

	/**
	 * Retrieves the list of predecessors for the current object.
	 *
	 * @return The array of strings representing the predecessors of the object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
