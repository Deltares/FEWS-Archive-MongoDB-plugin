package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class OutputPowerQuerySql {
	@QueryMapping
	public Document outputPowerQuerySqlById(@Argument String _id, DataFetchingEnvironment e){
		var r = Mongo.findOne("output.PowerQuerySql", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
		r.put("Expression", String.join("\n", r.getList("Expression", String.class)));
		return r;
	}
	
	@QueryMapping
	public Document outputPowerQuerySqlByStudyNameMonth(@Argument String study, @Argument String name, @Argument String month, DataFetchingEnvironment e){
		var r = Mongo.findOne("output.PowerQuerySql", new Document("Study", study).append("Name", name).append("Month", month), Conversion.getProjection(e));
		r.put("Expression", String.join("\n", r.getList("Expression", String.class)));
		return r;
	}

	@QueryMapping
	public List<Document> outputPowerQuerySqlNByStudy(@Argument String study, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuerySql", new Document("Study", study), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputPowerQuerySqlNByStudyName(@Argument String study, @Argument String name, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuerySql", new Document("Study", study).append("Name", name), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputPowerQuerySqlN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuerySql", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createOutputPowerQuerySql(@Argument String study, @Argument String name, @Argument String month, @Argument String expression){
		return Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(expression.split("\n")).collect(Collectors.toList()))).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateOutputPowerQuerySql(@Argument String _id, @Argument String study, @Argument String name, @Argument String month, @Argument String expression){
		return Mongo.updateOne("output.PowerQuerySql", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(expression.split("\n")).collect(Collectors.toList())))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteOutputPowerQuerySql(@Argument String _id){
		return Mongo.deleteOne("output.PowerQuerySql", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
