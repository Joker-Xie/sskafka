package com.meicloud.flink.table.source;

import org.apache.flink.table.sources.StreamTableSource;

public interface CriterStreamTabelFatory {

    StreamTableSource getTableSource();
}
