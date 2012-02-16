package com.inadco.ecoadapters.pig;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pass thru the incoming non-null tuple. If input is null, map it into a tuple
 * described by the supplied protobuf schema, with all attributes being null.
 * otherwise output attribute as a pass-thru value cast to the supplied schema
 * type. (warning: there's very little check for correspondence of the schemas
 * of the input and output!)
 * <P>
 * 
 * Q: What problems does it solve?
 * <P>
 * 
 * A: Pig 8's FLATTEN(a) inconsistency. Indeed, when a has a schema with more
 * than one attribute (say n attributes), and a row comes in having a = null,
 * then FLATTEN(a) produces a single NULL too, whereas it really should be
 * producing n nulls. After that, the data 2 schema mapping (naturally) fails
 * for the rest of attributes in the record (PIG-??? issue filed).
 * <P>
 * 
 * At the time of this writing, allegedly pig 11 will have a capability to
 * broadcast schema to the backend (perhaps including FLATTEN(X) operator too)
 * so this transformation can be carried out correctly. For the time being it's
 * a workaround using define and constructor params.
 * <P>
 * 
 * @author dmitriy
 * 
 */

public class Null2Pig extends Proto2Pig {

    protected int numFields;

    public Null2Pig(String msgDescString) {
        super(msgDescString);
        numFields = msgDesc.getFields().size();
    }

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        tuple=(Tuple) (tuple==null?null:tuple.get(0));
        if (tuple != null) {
            if ( tuple.size()!=numFields+1 ) 
                throw new IOException ("Null2Pig: incoming tuple doesn't conform to expected output schema");
            return tuple;
        }
        return PigUtil.emptyTuple(msgDesc, tupleFactory);
    }

    @Override
    public Schema outputSchema(Schema input) {
        List<FieldSchema> fields = input.getFields();
        if (fields.size() != 1)
            throw new RuntimeException("Wrong # of arguments in call to ProtoToPig()");

        FieldSchema argSchema = fields.get(0);
        if ( argSchema.type!=DataType.TUPLE )
            throw new RuntimeException (String.format("1 tuple argument expected."));
        
        if (argSchema.schema.size() != pigSchema.size()) {
            throw new RuntimeException(String.format("Null2Pig: Input and output schema do not match for msg '%s'",
                                                     msgDesc.getName()));
        }

        return pigSchema;
    }

}
