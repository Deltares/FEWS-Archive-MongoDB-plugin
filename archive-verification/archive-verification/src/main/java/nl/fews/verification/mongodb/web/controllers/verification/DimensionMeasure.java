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
public class DimensionMeasure {
	@QueryMapping
	public Document dimensionMeasureById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("dimension.Measure", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
	
	@QueryMapping
	public Document dimensionMeasureByMeasureId(@Argument String measureId, DataFetchingEnvironment e){
		return Mongo.findOne("dimension.Measure", new Document("measureId", measureId), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> dimensionMeasureN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("dimension.Measure", new Document(), Conversion.getProjection(e)).sort(new Document("measureId", 1)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createDimensionMeasure(@Argument String measureId, @Argument String measure, @Argument Integer perfectScore){
		return Mongo.insertOne("dimension.Measure", new Document("measureId", measureId).append("measure", measure).append("perfectScore", perfectScore)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateDimensionMeasure(@Argument String _id, @Argument String measureId, @Argument String measure, @Argument Integer perfectScore){
		return Mongo.updateOne("dimension.Measure", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("measureId", measureId).append("measure", measure).append("perfectScore", perfectScore))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteDimensionMeasure(@Argument String _id){
		return Mongo.deleteOne("dimension.Measure", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
