package run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

public class DO {
    static Class<? extends List> LINKDATA_CLASS = new ArrayList<String>().getClass();
    private static final Charset CHARSET = Charset.forName("UTF-8");

    List<Document> prev_bulk;
    private Cipher encCipher;
    private Cipher decCipher;
    private Mac macInstance;
    String LINK_LABEL = "Link_Data", ID_LABEL = "unique_key", AUTH_LABEL = "hmac",
            COLLECTION_NAME = "runcollection", AUTH_KEY = "auth_key", ENC_KEY = "0123456789123456";
    int LINK_NUMBER = 4;
    int BULK_SIZE = 30;
    List<Document> buffer = new ArrayList<>();
    Random RAND = new Random();
    DOConnection connection;

    DO( DOConnection connection) {
        this.connection = connection;
        try {
            macInstance = Mac.getInstance("HmacSHA256");
            SecretKeySpec km = new SecretKeySpec(AUTH_KEY.getBytes(), "HmacSHA256");
            macInstance.init(km);
            SecretKey ke = new SecretKeySpec(ENC_KEY.getBytes(), "AES");
            encCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            encCipher.init(Cipher.ENCRYPT_MODE, ke);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (NoSuchPaddingException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }

    }
    public void Insert(Document insert_data) {
        buffer.add(insert_data);
        if (buffer.size() == BULK_SIZE) {
            List<Document> prev_bulk = new ArrayList<>();
            PutBulk(buffer, prev_bulk);
            prev_bulk = buffer;
            buffer = new ArrayList<>();
        }
    }
    private void PutBulk(List<Document> current_bulk, List<Document> prev_bulk) {
        Map<String, Document> tuples = new HashMap<>();
        for (Document doc : prev_bulk)
            tuples.put(doc.getString(ID_LABEL), doc);
        for (Document doc : current_bulk)
            tuples.put(doc.getString(ID_LABEL), doc);

        List<String> id_set = new ArrayList<>();
        for (Document doc : current_bulk)
            id_set.add(doc.getString(ID_LABEL));

        for (Document doc : prev_bulk) {
            List<String> link_set  = GenLink(doc.getString(ID_LABEL), id_set);
            String to = doc.getString(ID_LABEL);
            for (String from : link_set) {
                Document linking_doc = tuples.get(from);
                AppendLinkData(linking_doc, to);
            }
        }

        for (Document doc : current_bulk) {
            List<String> link_set  = GenLink(doc.getString(ID_LABEL), id_set);
            String to = doc.getString(ID_LABEL);
            for (String from : link_set) {
                Document linking_doc = tuples.get(from);
                AppendLinkData(linking_doc, to);
            }
        }
        for (Document doc : current_bulk)
            EncDocument(doc);
        connection.PutBulk(COLLECTION_NAME ,current_bulk);
    }
    List<String> GenLink(String to, List<String> link_from) {
        List<String> result = new ArrayList<>();
        while (result.size() < LINK_NUMBER/2) {
            String nextKey = link_from.get(RAND.nextInt(link_from.size()));
            if (!nextKey.equals(to)) {
                result.add(nextKey);
            }
        }
        return result;
    }

    void EncDocument(Document doc){
        if (!doc.containsKey(LINK_LABEL)) return;
        ObjectMapper objectMapper = new ObjectMapper();
        String link_data = null;
        try {
            link_data = objectMapper.writeValueAsString(doc.get(LINK_LABEL, LINKDATA_CLASS));
            doc.replace(LINK_LABEL, Enc(link_data));
            System.out.println(doc.toString());
            doc.append(AUTH_LABEL, Auth(doc));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    String Enc(String data) {
        try {
            String enc_data = Base64.getEncoder().encodeToString(encCipher.doFinal(data.getBytes(CHARSET)));
            return enc_data;
        } catch (IllegalBlockSizeException e) {
            throw new RuntimeException(e);
        } catch (BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
    String Auth(Document doc) {
        String auth_data = new String( macInstance.doFinal(doc.toString().getBytes()));
        return auth_data;
    }
    void AppendLinkData(Document doc, String link_to) {
        if (!doc.containsKey(LINK_LABEL)) {
            List<String> link_data = new ArrayList<>();
            link_data.add(link_to);
            doc.append(LINK_LABEL, link_data);
            return;
        }
        List<String> link_data = doc.get(LINK_LABEL, LINKDATA_CLASS);
        link_data.add(link_to);
        doc.replace(LINK_LABEL, link_data);
    }
}
