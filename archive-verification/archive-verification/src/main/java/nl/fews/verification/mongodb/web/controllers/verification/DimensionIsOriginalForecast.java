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
import java.util.stream.StreamSupport;

@Controller
public class DimensionIsOriginalForecast {
	@QueryMapping
	public Document dimensionIsOriginalForecastById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("dimension.IsOriginalForecast", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
	
	@QueryMapping
	public List<Document> dimensionIsOriginalForecastN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("dimension.IsOriginalForecast", new Document(), Conversion.getProjection(e)).sort(new Document("isOriginalForecast", 1)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createDimensionIsOriginalForecast(@Argument String isOriginalForecast){
		return Mongo.insertOne("dimension.IsOriginalForecast", new Document("IsOriginalForecast", isOriginalForecast)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateDimensionIsOriginalForecast(@Argument String _id, @Argument String isOriginalForecast){
		return Mongo.updateOne("dimension.IsOriginalForecast", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("isOriginalForecast", isOriginalForecast))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteDimensionIsOriginalForecast(@Argument String _id){
		return Mongo.deleteOne("dimension.IsOriginalForecast", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
