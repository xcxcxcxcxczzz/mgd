package run;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.nio.charset.Charset;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Client {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    static Class<? extends List> LINKDATA_CLASS = new ArrayList<String>().getClass();
    Set<String> modified = new HashSet<>();
    Set<String> missing = new HashSet<>();
    ClientConnection connection;
    private Cipher decCipher;
    private Mac macInstance;
    String LINK_LABEL = "Link_Data", ID_LABEL = "unique_key", AUTH_LABEL= "hmac",
            COLLECTION_NAME = "runcollection", AUTH_KEY = "auth_key", ENC_KEY = "0123456789123456";

    Client( ClientConnection connection) {
        this.connection = connection;
        try {
            macInstance = Mac.getInstance("HmacSHA256");
            SecretKeySpec km = new SecretKeySpec(AUTH_KEY.getBytes(), "HmacSHA256");
            macInstance.init(km);
            SecretKey ke = new SecretKeySpec(ENC_KEY.getBytes(), "AES");
            decCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            decCipher.init(Cipher.DECRYPT_MODE, ke);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (NoSuchPaddingException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
    List<Document> read(Bson query){
        List<Document> docs = connection.Read(COLLECTION_NAME,query);
        Set<String> linked_ids = new HashSet<>();
        for (Document doc : docs) {
            Auth(doc);
            linked_ids.addAll(ExtractLink(doc));
        }
        VerifyLink(linked_ids);
        return docs;
    }
    void Auth(Document doc) {
        String mac_data = doc.getString(AUTH_LABEL);
        doc.remove(AUTH_LABEL);
        String s = new String( macInstance.doFinal(doc.toString().getBytes()));
        if (!s.equals(mac_data)) {
            modified.add(doc.getString(ID_LABEL));
            System.out.println("fail auth " + modified.size());
            System.out.println("1" + s);
            System.out.println("2" + mac_data);
        }
    }
    List<String> ExtractLink(Document doc) {
        if (!doc.containsKey(LINK_LABEL)) return new ArrayList<>();
        String data = doc.getString(LINK_LABEL);
        try {
            String dec_data = new String(decCipher.doFinal(Base64.getDecoder().decode(data)), CHARSET);
            List<String> link_data = Document.parse("{\"list\":" + dec_data +"}").get("list", LINKDATA_CLASS);
            doc.remove(LINK_LABEL);
            System.out.println(doc.toString());
            return  link_data;
        } catch (IllegalBlockSizeException e) {
            throw new RuntimeException(e);
        } catch (BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
    void VerifyLink(Set<String> ids) {
        Iterator<String> iterator = ids.iterator();
        while (iterator.hasNext()) {
            String id = iterator.next();
            Bson query = Filters.eq(ID_LABEL, id);
            List<Document> retrieved =  connection.Read(COLLECTION_NAME, query);
            if (retrieved.isEmpty()) {
                missing.add(id);
                System.out.println("fail link");

            }
        }
    }
    public int ModifiedCount(){return modified.size();}
    public int MissingCount(){return missing.size();}

}
