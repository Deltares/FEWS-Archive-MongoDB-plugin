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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class OutputPowerQuery {
	@QueryMapping
	public Document outputPowerQueryById(@Argument String _id, DataFetchingEnvironment e){
		var r = Mongo.findOne("output.PowerQuery", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
		r.put("Expression", String.join("\n", r.getList("Expression", String.class)));
		return r;
	}
	
	@QueryMapping
	public Document outputPowerQueryByStudyNameMonth(@Argument String study, @Argument String name, @Argument String month, DataFetchingEnvironment e){
		var r = Mongo.findOne("output.PowerQuery", new Document("Study", study).append("Name", name).append("Month", month), Conversion.getProjection(e));
		r.put("Expression", String.join("\n", r.getList("Expression", String.class)));
		return r;
	}

	@QueryMapping
	public List<Document> outputPowerQueryNByStudy(@Argument String study, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuery", new Document("Study", study), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputPowerQueryNByStudyName(@Argument String study, @Argument String name, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuery", new Document("Study", study).append("Name", name), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputPowerQueryN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.PowerQuery", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).peek(r -> r.put("Expression", String.join("\n", r.getList("Expression", String.class)))).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createOutputPowerQuery(@Argument String study, @Argument String name, @Argument String month, @Argument String expression){
		return Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(expression.split("\n")).collect(Collectors.toList()))).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateOutputPowerQuery(@Argument String _id, @Argument String study, @Argument String name, @Argument String month, @Argument String expression){
		return Mongo.updateOne("output.PowerQuery", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(expression.split("\n")).collect(Collectors.toList())))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteOutputPowerQuery(@Argument String _id){
		return Mongo.deleteOne("output.PowerQuery", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
