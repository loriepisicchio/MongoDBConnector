/**
 * 
 */
package com.motwin.sample.mongodb;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.motwin.streamdata.Jsons;
import com.motwin.streamdata.spi.SourceMetadata;
import com.motwin.streamdata.spi.impl.SourceMetadataImpl;

/**
 * This Source will retreive data from a MongoDB table.
 * 
 */
public class MyTableSource extends AbstractMongoDBSource {

    /**
     * MyTableSource constructor.
     * 
     */
    public MyTableSource(final String aMongoHost, final Integer aMongoPort, final String aDbName) {
        super(aMongoHost, aMongoPort, aDbName);
    }

    @Override
    public SourceMetadata getMetadata() {
        return new SourceMetadataImpl(ImmutableMap.<String, Boolean> of());
    }

    @Override
    public String getSourceType() {
        return "MongoDB pollable source";
    }

    @Override
    protected DBCursor performQuery(DB aDatabase) {
        Preconditions.checkNotNull(aDatabase, "aDatabase cannot be null");

        // This method will fetch all elements of collection "myTable"
        
        // TODO : adapt this to perform the query matching your needs
        DBCollection collection = aDatabase.getCollection("myTable");

        DBCursor cursor = collection.find();

        return cursor;
    }

    @Override
    protected ObjectNode mapMongoDbRowToVirtualTableRow(DBObject aMongoRow) {
        Preconditions.checkNotNull(aMongoRow, "aMongoRow cannot be null");

        ObjectNode row;
        row = Jsons.newObject();

        // Each element will be mapped in a virtual table row. 
        // In this sample, the table has one column for the id, one for the content.
        
        // TODO : adapt this to the schema of your virtual table, and the content of the MongoDB collection
        row.put("id", aMongoRow.get("_id").toString());
        row.put("content", aMongoRow.toString());

        return row;
    }

}
