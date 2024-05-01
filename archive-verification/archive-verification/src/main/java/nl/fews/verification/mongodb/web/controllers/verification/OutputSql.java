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
import java.util.stream.StreamSupport;

@Controller
public class OutputSql {
	@QueryMapping
	public Document outputSqlById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("output.Sql", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
	
	@QueryMapping
	public Document outputSqlByStudyNameMonth(@Argument String study, @Argument String name, @Argument String month, DataFetchingEnvironment e){
		return Mongo.findOne("output.Sql", new Document("Study", study).append("Name", name).append("Month", month), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> outputSqlNByStudy(@Argument String study, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.Sql", new Document("Study", study), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).toList();
	}

	@QueryMapping
	public List<Document> outputSqlNByStudyName(@Argument String study, @Argument String name, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.Sql", new Document("Study", study).append("Name", name), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).toList();
	}

	@QueryMapping
	public List<Document> outputSqlN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.Sql", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createOutputSql(@Argument String study, @Argument String name, @Argument String month, @Argument String query){
		return Mongo.insertOne("output.Sql", new Document("Study", study).append("Name", name).append("Month", month).append("Query", query)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateOutputSql(@Argument String _id, @Argument String study, @Argument String name, @Argument String month, @Argument String query){
		return Mongo.updateOne("output.Sql", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Study", study).append("Name", name).append("Month", month).append("Query", query))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteOutputSql(@Argument String _id){
		return Mongo.deleteOne("output.Sql", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
