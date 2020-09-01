package com.alibaba.datax.plugin.writer.solrwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Date: 2020/8/27
 * @Copyright（C）: 2014-2020 X-Financial Inc.   All rights reserved.
 * 注意：本内容仅限于小赢科技有限责任公司内部传阅，禁止外泄以及用于其他的商业目的。
 */
public class SolrWriter {
    private final static String WRITE_COLUMNS = "column";

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post() {}

        @Override
        public void destroy() {}
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;

        private String zkQuorum;
        private String collectionName;
        private Integer zkConnectTimeout;
        private Integer zkClientTimeout;
        private String idColumn;

        CloudSolrClient solrClient = null;

        private List<SolrFieldType> typeList;
        private List<SolrColumn> columnList;

        private int trySize;
        private int batchSize;
        private String splitter;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            zkQuorum = Key.getZkQuorum(conf);
            collectionName = Key.getCollectionName(conf);
            zkConnectTimeout = Key.getZkConnectTimeout(conf);
            zkClientTimeout = Key.getZkClientTimeout(conf);

            idColumn = Key.getIdColumn(conf);
            trySize = Key.getTrySize(conf);
            batchSize = Key.getBatchSize(conf);
            splitter = Key.getSplitter(conf);
            columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<SolrColumn>>() {
            });

            typeList = new ArrayList<>(columnList.size());
            for (SolrColumn col : columnList) {
                typeList.add(SolrFieldType.getESFieldType(col.getType()));
            }

            solrClient = new CloudSolrClient(zkQuorum);
            solrClient.setDefaultCollection(collectionName);
            solrClient.setZkConnectTimeout(zkConnectTimeout);
            solrClient.setZkClientTimeout(zkClientTimeout);

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writesize", String.valueOf(total));
            log.info(msg);
            try {
                solrClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private long doBatchInsert(final List<Record> writerBuffer) {
            List<SolrInputDocument> docs = new ArrayList<>(batchSize);

            //final Bulk.Builder bulkaction = new Bulk.Builder().defaultIndex(this.index).defaultType(this.type);
            for (Record record : writerBuffer) {
                SolrInputDocument doc = new SolrInputDocument();
                String id = null;
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String columnName = columnList.get(i).getName();
                    SolrFieldType columnType = typeList.get(i);

                    //log.info("column: {}, columnName: {}, columnType: {}", column, columnName, columnType);

                    if (columnName.equals(idColumn)) {
                        doc.setField("id", column.asString());
                    }

                    switch (columnType) {
                        case BOOLEAN:
                            doc.setField(columnName, column.asBoolean());
                            break;
                        case BYTE:
                        case BINARY:
                            doc.setField(columnName, column.asBytes());
                            break;
                        case SHORT:
                        case INTEGER:
                        case LONG:
                        case BIGINT:
                            doc.setField(columnName, column.asLong());
                            break;
                        case DECIMAL:
                            //doc.setField(columnName, column.asBigDecimal());
                            //break;
                        case FLOAT:
                        case DOUBLE:
                            doc.setField(columnName, column.asDouble());
                            break;
                        default:
                            String value = column.asString();
                            if (value != null && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")) {
                                value = value.trim().replace(" ", "T") + "Z";
                            }
                            doc.setField(columnName, value);
                            break;
                    }
                }

                //log.info("doc: {}", doc);
                docs.add(doc);
            }

            try {
                return RetryUtil.executeWithRetry(() -> {
                    solrClient.add(docs);
                    UpdateResponse response = solrClient.commit(true, true, true);
                    docs.clear();
                    return response.getResponse().size();
                }, trySize, 60000L, true);
            } catch (Exception e) {
                if (Key.isIgnoreWriteError(this.conf)) {
                    log.warn(String.format("重试[%d]次写入失败，忽略该错误，继续写入!", trySize));
                } else {
                    throw DataXException.asDataXException(SolrWriterErrorCode.SOLR_INDEX_INSERT, e);
                }
            }
            return 0;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy(){
            try {
                solrClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
