package nl.fews.verification.mongodb.generate.operations.sql;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import org.bson.Document;

public class Sql {
	private Sql(){}

	public static void execute(){
		Mongo.deleteMany("output.Sql", new Document());
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Sql.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
