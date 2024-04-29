package nl.fews.verification.mongodb.generate.operations.cube;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import org.bson.Document;

public class Cube {
	private Cube(){}

	public static void execute(){
		Mongo.deleteMany("output.Cube", new Document());
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Cube.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
