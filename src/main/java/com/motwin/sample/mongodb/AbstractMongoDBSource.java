/**
 * 
 */
package com.motwin.sample.mongodb;

import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.motwin.streamdata.Jsons;
import com.motwin.streamdata.spi.PollableSource;
import com.motwin.streamdata.spi.SourceException;

/**
 * This Source will retreive data from a MongoDB database.
 * 
 */
public abstract class AbstractMongoDBSource implements PollableSource {

    private final String  mongoHost;
    private final Integer mongoPort;
    private final String  dbName;

    /**
     * MongoDBSource constructor.
     * 
     */
    public AbstractMongoDBSource(final String aMongoHost, final Integer aMongoPort, final String aDbName) {
        Preconditions.checkNotNull(aMongoHost, "aMongoHost cannot be null");
        Preconditions.checkNotNull(aMongoPort, "aMongoPort cannot be null");
        Preconditions.checkNotNull(aDbName, "aDbName cannot be null");

        mongoHost = aMongoHost;
        mongoPort = aMongoPort;
        dbName = aDbName;
    }

    @Override
    public final Callable<JsonNode> execute(JsonNode aArg0) throws Exception {
        final DB database;
        try {
            database = getDatabase(mongoHost, mongoPort, dbName);
            return new Callable<JsonNode>() {

                @Override
                public JsonNode call() throws Exception {
                    DBCursor cursor = performQuery(database);

                    ArrayNode result;
                    result = Jsons.newArray();

                    try {
                        while (cursor.hasNext()) {
                            ObjectNode resultRow;
                            resultRow = mapMongoDbRowToVirtualTableRow(cursor.next());

                            result.add(resultRow);
                        }
                    } catch (Exception e) {
                        throw new SourceException("Unable to perform query", e);
                    } finally {
                        cursor.close();
                    }

                    return result;

                }
            };

        } catch (Exception e) {
            throw new SourceException("Unable to connect to database", e);
        }
    };

    private static DB getDatabase(String aMongoDbHost, Integer aMongoDbPort, String aDbName)
            throws UnknownHostException {
        Preconditions.checkNotNull(aMongoDbHost, "aMongoDbHost cannot be null");
        Preconditions.checkNotNull(aMongoDbPort, "aMongoDbPort cannot be null");
        Preconditions.checkNotNull(aDbName, "aDbName cannot be null");

        DB db;

        Mongo mongoClient = new MongoClient(aMongoDbHost, aMongoDbPort);
        db = mongoClient.getDB(aDbName);

        return db;
    }

    protected abstract DBCursor performQuery(DB aDatabase);

    protected abstract ObjectNode mapMongoDbRowToVirtualTableRow(DBObject aMongoRow);

}
