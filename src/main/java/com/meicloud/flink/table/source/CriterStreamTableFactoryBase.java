package com.meicloud.flink.table.source;

import com.meicloud.flink.table.source.descriptor.TableSoueceDescriptor;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.table.sources.StreamTableSource;

public class CriterStreamTableFactoryBase implements CriterStreamTabelFatory {

    private InputKafkaConfigVo inputConfigVo;

    public CriterStreamTableFactoryBase(InputKafkaConfigVo inputConfigVo) {
        this.inputConfigVo = inputConfigVo;
    }

    @Override
    public StreamTableSource getTableSource() {
        String sourceType = inputConfigVo.sourceType();
        switch (sourceType) {
            case "kafka":
                return new LocalKafkaTableSource(inputConfigVo);
            case "mysql":
                return null;
//                return new LocalMysqlTableSource();
            case "Es":
                return null;
//                return new LocalEsTableSource();
            case "file":
                return null;
//                return new LocalFileTableSource();
            default:
                try {
                    throw new Exception("暂不支持该类型数据源.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
        return null;
    }
}
