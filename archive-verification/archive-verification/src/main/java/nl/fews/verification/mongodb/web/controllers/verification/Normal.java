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

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

@Controller
public class Normal {
	@QueryMapping
	public Document normalById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("Normal", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document normalByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("Normal", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> normalN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Normal", new Document(), Conversion.getProjection(e)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createNormal(@Argument String name, @Argument String collection, @Argument int generateDays, @Argument List<Map<String, Object>> filters){
		return Mongo.insertOne("Normal", new Document("Name", name).append("Collection", collection).append("GenerateDays", generateDays).append("Filters", filters)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateNormal(@Argument String _id, @Argument String name, @Argument String collection, @Argument int generateDays, @Argument List<Map<String, Object>> filters){
		return Mongo.updateOne("Normal", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Name", name).append("Collection", collection).append("GenerateDays", generateDays).append("Filters", filters))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteNormal(@Argument String _id){
		return Mongo.deleteOne("Normal", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
