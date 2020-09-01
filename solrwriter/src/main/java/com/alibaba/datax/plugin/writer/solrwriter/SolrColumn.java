package com.alibaba.datax.plugin.writer.solrwriter;

/**
 * @Date: 2020/8/27
 * @Copyright（C）: 2014-2020 X-Financial Inc.   All rights reserved.
 * 注意：本内容仅限于小赢科技有限责任公司内部传阅，禁止外泄以及用于其他的商业目的。
 */
public class SolrColumn {
    private String name;

    private String type;

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
