package run;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;

public class Run {
    private static final Random rand = new Random();

    public static void main(String[] args) {
        System.setProperty("mongodb.uri" ,"mongodb://localhost:27017");
        ClientConnection clientConnection = new ClientConnection();
        DOConnection doConnection = new DOConnection();
        DO data_owner = new DO(doConnection);
        for (int i=0; i<300; i++) {
            Document doc = GenerateDocument(i);
            data_owner.Insert(doc);
        }
        Client client = new Client(clientConnection);
        Bson query = eq("unique_key", "3");
        List<Document> read_data =  client.read(query);
    }

    static Document GenerateDocument(int unique_key) {
        return new Document("type", "exam").append("score", rand.nextDouble() * 100).append("unique_key",""+unique_key);
    }
}
