package run;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import java.util.List;

public class DOConnection {
    MongoClient mongoClient;
    MongoDatabase mongoDatabase;
    String connectionString, dbname;

    DOConnection(){
        this.connectionString = System.getProperty("mongodb.uri");
        this.dbname = "rundb";
    }
    void CreateUniqueField(String collection_name, String field_name) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collection_name);
        Document index = new Document(field_name, 1);
        collection.createIndex(index, new IndexOptions().unique(true));
    }
    void PutBulk(String collection_name ,List<Document> bulk) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("rundb");
            MongoCollection<Document> collection = sampleTrainingDB.getCollection(collection_name);
          collection.insertMany(bulk);
    }
    }
}
