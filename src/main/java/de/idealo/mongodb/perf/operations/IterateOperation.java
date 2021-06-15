package de.idealo.mongodb.perf.operations;

import com.mongodb.client.MongoCursor;
import de.idealo.mongodb.perf.MongoDbAccessor;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;
//import static com.mongodb.client.model.Filters.in;

/**
 * Created by kay.agahd on 23.11.16.
 */
public class IterateOperation extends AbstractOperation {

    private final String collection;

    public IterateOperation(MongoDbAccessor mongoDbAccessor, String db, String collection, String field){
        super(mongoDbAccessor, db, collection, field);
        this.collection = collection;
    }

    @Override
    long executeQuery(int threadId, long threadRunCount, long globalRunCount, long selectorId, long randomId){
        if (collection.equals("bookings")) {
            return executeBookingsQuery();
        }

        return executePerfQuery(selectorId);
    }

    @Override
    public OperationModes getOperationMode(){
        if(IOperation.THREAD_RUN_COUNT.equals(queriedField)) return OperationModes.ITERATE_MANY;
        else return OperationModes.ITERATE_ONE;
    }

    private long executePerfQuery(long selectorId) {
        final MongoCursor<Document> cursor = mongoCollection.find(eq(queriedField, selectorId)).iterator();
        //final MongoCursor<Document> cursor = mongoCollection.find(in(queriedField, selectorId, selectorId+1, selectorId+2, selectorId+3, selectorId+4)).iterator();
        long result = 0;
        try {
            while (cursor.hasNext()) {
                final Document doc = cursor.next();
                LOG.debug("Document {}", doc.toJson());
                result++;
            }
        } finally {
            cursor.close();
        }

        return result;
    }

    private long executeBookingsQuery() {
        executeOrderNumberQuery();
        return executePosornQuery();
    }

    private long executeOrderNumberQuery() {
        String orderNumberField = "orderNumber";

        final MongoCursor<Document> onCursor = mongoCollection.find(eq(orderNumberField, getOrderNumber())).iterator();

        long onResult = 0;
        try {
            while (onCursor.hasNext()) {
                final Document doc = onCursor.next();
                LOG.debug("Document {}", doc.toJson());
                onResult++;
            }
        } finally {
            onCursor.close();
        }

        return onResult;
    }

    private long executePosornQuery() {
        String posornField = "pointOfSaleOrderReferenceNumber";
        final MongoCursor<Document> posornCursor;

        try {
            posornCursor = mongoCollection.find(eq(posornField, getPosorn())).iterator();
        }  catch (Exception ex) {
            LOG.error("exception occurred | ex={}", ex.getMessage());
            return 0L;
        }

        long result = 0;
        try {
            while (posornCursor.hasNext()) {
                final Document doc = posornCursor.next();
                LOG.debug("Document {}", doc.toJson());
                result++;
            }
        } finally {
            posornCursor.close();
        }

        return result;
    }

    private Long getOrderNumber() {
        List<Long> givenList = Arrays.asList(
                6684138837L, 9272898358L, 4876024176L, 5000604463L, 4957493444L, 3991012424L, 9633337830L,
                6505671110L, 6217164080L, 2427382970L, 1356266722L, 9565229282L, 1574484707L, 4766314760L,
                6305166545L, 8721384720L, 4312197158L, 7615239473L, 3562693945L, 9822032877L, 2813237262L
        );
        Random rand = new Random();
        return givenList.get(rand.nextInt(givenList.size()));
    }

    private String getPosorn() {
        List<String> givenList = Arrays.asList(
                "agQM0aaaaa", "gLQGlcaaaa", "SkMNWbaaaa", "kKv4kdaaaa", "nirvycaaaa", "fNXW-baaaa", "5L7RGbaaaa",
                "mVZQBbaaaa", "A3B1Ocaaaa", "58B85caaaa", "foGEfdaaaa", "yX11Cbaaaa", "6nGS7baaaa", "wVxo-baaaa",
                "bNCp-caaaa", "Te2hHdaaaa", "zbphPdaaaa", "M9VmFaaaaa", "Er5XDaaaaa", "EGL2Gdaaaa"
        );
        Random rand = new Random();
        return givenList.get(rand.nextInt(givenList.size()));
    }
}
