package nl.fews.verification.mongodb.generate.operations.data;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import org.bson.Document;

public class Data {
	private Data(){}

	public static void execute(){
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Data.class, new Object[]{study.getString("Name")})).forEach(Execute::execute)
		);
	}
}
