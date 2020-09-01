package com.alibaba.datax.plugin.writer.solrwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @Date: 2020/8/27
 * @Copyright（C）: 2014-2020 X-Financial Inc.   All rights reserved.
 * 注意：本内容仅限于小赢科技有限责任公司内部传阅，禁止外泄以及用于其他的商业目的。
 */
public enum SolrWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("SOLRWriter-00", "您配置的值不合法."),
    SOLR_INDEX_DELETE("SOLRWriter-01", "删除index错误."),
    SOLR_INDEX_CREATE("SOLRWriter-02", "创建index错误."),
    SOLR_MAPPINGS("SOLRWriter-03", "mappings错误."),
    SOLR_INDEX_INSERT("SOLRWriter-04", "插入数据错误."),
    SOLR_ALIAS_MODIFY("SOLRWriter-05", "别名修改错误."),
    ;

    private final String code;
    private final String description;

    SolrWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
