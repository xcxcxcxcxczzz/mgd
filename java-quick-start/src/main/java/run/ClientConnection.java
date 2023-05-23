package run;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class ClientConnection {
    MongoClient mongoClient;
    MongoDatabase mongoDatabase;
    boolean attack_enabled = false;
    ClientConnection () {
        String connectionString = System.getProperty("mongodb.uri");
        String dbname = "rundb";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoDatabase = mongoClient.getDatabase(dbname);
            this.mongoClient = mongoClient;
        }
    }
    void Attack(List<Document> docs) {
    }
    public List<Document> Read(String collection_name, Bson query) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("rundb");
            MongoCollection<Document> collection = sampleTrainingDB.getCollection(collection_name);
            FindIterable<Document> iterable = collection.find(query);
            List<Document> retrieved = new ArrayList<>();
            iterable.into(retrieved);
            if (attack_enabled) Attack(retrieved);
            for (Document doc : retrieved)
                doc.remove("_id");
            return retrieved;
        }
    }
}
