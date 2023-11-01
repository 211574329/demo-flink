package com.echo.function;

import com.echo.poly.OrderRanking;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class AreaProductSlink<T> extends RichSinkFunction<OrderRanking> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(OrderRanking value, Context context) throws Exception {
        super.invoke(value, context);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
