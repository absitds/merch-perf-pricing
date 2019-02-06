package com.albertsons.itds;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Pig udf.
 */
public class Main extends EvalFunc<String> {

    /**
     *
     * @param tuple
     * @return
     * @throws IOException
     */
    @Override
    public String exec(final Tuple tuple) throws IOException {
        //place code here
        return null;
    }
}