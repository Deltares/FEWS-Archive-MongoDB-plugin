package nl.fews.verification.mongodb.generate.operations.cube.tabular;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.DataSources;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.Roles;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.Tables;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

public final class Model implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Model(String study){
		this.study = study;
	}

	/**
	 * Executes the verification process for a given study.
	 * Retrieves the study document from the database using the provided study name.
	 * Retrieves the template document from the database using the cube name from the study document.
	 * Appends the necessary fields to the template document for verification.
	 * Generates the data sources, tables, and roles for the verification using the template document.
	 * Inserts the study and the template document into the output.Cube collection.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		Document template = Mongo.findOne("template.Cube", new Document("Name", studyDocument.getString("Cube"))).get("Template", Document.class);

		template.append("id", String.format("Verification_%s", study));
		template.append("name", String.format("Verification_%s", study));
		template.get("model", Document.class).append("name", String.format("Verification_%s", study));

		new DataSources(template).generate();
		new Tables(studyDocument, template).generate();
		new Roles(template).generate();

		Mongo.insertOne("output.Cube", new Document("Name", study).append("Bim", template));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
