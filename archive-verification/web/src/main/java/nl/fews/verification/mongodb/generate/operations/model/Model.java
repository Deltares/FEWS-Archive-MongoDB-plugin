package nl.fews.verification.mongodb.generate.operations.model;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import org.bson.Document;

public class Model {
	private Model(){}

	/**
	 * Executes the method to perform a series of actions.
	 * This method deletes certain documents from a MongoDB collection and then
	 * performs some operations on a set of objects retrieved from another MongoDB
	 * collection.
	 */
	public static void execute(){
		Mongo.deleteMany("output.PowerQuery", new Document());
		Mongo.find("Study", new Document()).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Model.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
