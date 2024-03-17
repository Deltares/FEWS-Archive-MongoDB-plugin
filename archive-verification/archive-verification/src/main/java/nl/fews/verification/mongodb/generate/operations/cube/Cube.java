package nl.fews.verification.mongodb.generate.operations.cube;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import org.bson.Document;

public class Cube {
	private Cube(){}

	/**
	 * Executes the Cube algorithm.
	 *
	 * This method deletes the output.Cube documents from the Verification collection in the MongoDB.
	 * It then retrieves all Study documents from the Verification collection and for each study,
	 * it applies the Cube algorithm by generating and executing a directed acyclic graph (DAG) of Cube instances.
	 * The DAG is constructed with the study name as a parameter.
	 *
	 */
	public static void execute(){
		Mongo.deleteMany("output.Cube", new Document());
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Cube.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
