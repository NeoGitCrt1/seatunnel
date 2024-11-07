/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.s3enc;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Slf4j
public class S3EncTransform extends AbstractCatalogSupportTransform {
    protected static final String PREFIX = "s3://";
    public static final String PLUGIN_NAME = "S3Enc";
    protected final String bucket;
    protected final String resize;
    protected final String splitBy;
    protected volatile transient OSSClient ossClient;
    protected final ReadonlyConfig config;
    public S3EncTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        ossClient = new OSSClient(config.get(S3EncTransformConfig.endpoint),
                config.get(S3EncTransformConfig.ak),
                config.get(S3EncTransformConfig.sk));
        this.bucket = config.get(S3EncTransformConfig.bucket);
        this.resize = config.get(S3EncTransformConfig.resize);
        this.splitBy = config.get(S3EncTransformConfig.split_by);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
    private String downEncode(String ossKey, Column fromColumn) {
        if (ossKey == null || ossKey.isEmpty()) {
            return "";
        }
        if (ossClient == null) {
            synchronized (PREFIX) {
                if (ossClient == null) {
                    ossClient = new OSSClient(config.get(S3EncTransformConfig.endpoint),
                            config.get(S3EncTransformConfig.ak),
                            config.get(S3EncTransformConfig.sk));
                }
            }
        }
        String[] splitKey = ossKey.split("@");
        String specBucket = splitKey.length > 1 ? splitKey[splitKey.length - 1] : this.bucket;
        Base64.Encoder encoder = Base64.getEncoder();

        try {
            GetObjectRequest request = new GetObjectRequest(specBucket, ossKey);
//            图片缩放为长边1000 px，即resize,l_1000。
            request.setProcess("image/resize,l_" + resize);
            OSSObject ossObject = ossClient.getObject(request);
            String b64 = encoder.encodeToString(IOUtils.readStreamAsByteArray(ossObject.getObjectContent()));
            String fileType = ossKey.substring(ossKey.lastIndexOf('.') + 1);
            return  "data:image/" + fileType + ";base64," + b64;
        } catch (Exception e) {
            log.error("文件下载/压缩/编码错误: {} >> {}", fromColumn.getName(), ossKey, e);
            return "";
        }
    }
    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {

        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        Object[] rowFields = inputRow.getFields();
        Object[] outFields = new Object[rowFields.length];
        for (int i = 0; i < rowFields.length; i++) {
            Column column = columns.get(i);
            Object rowField = rowFields[i];
            if (rowField == null) {
                continue;
            }
            switch (column.getDataType().getSqlType()) {
                case STRING:
                    if (((String)rowField).startsWith(PREFIX)) {
                        outFields[i] = downEncode(rowField.toString().substring(PREFIX.length()), column);
                    } else {
                        outFields[i] = rowField;
                    }
                    break;
                case ARRAY:
                    Object[] listValue = (Object[])rowField;
                    if (listValue != null && listValue.length > 0 && listValue[0] instanceof String) {
                        List<String> procList = new ArrayList<>(listValue.length);
                        for (int i1 = 0; i1 < listValue.length; i1++) {
                            String elm = (String) listValue[i1];
                            if (elm.startsWith(PREFIX)) {
                                String s3key = elm.substring(PREFIX.length());
                                if (splitBy.isEmpty()) {
                                    procList.add(downEncode(s3key, column));
                                } else {
                                    log.info("get ossKey for splitBy : {} >> {}" , column.getName(), s3key);
                                    String[] splitKeys = s3key.split(splitBy);
                                    for (String splitKey : splitKeys) {
                                        procList.add(downEncode(splitKey, column));
                                    }
                                }
                            } else {
                                procList.add(elm) ;
                            }
                        }
                        outFields[i] = procList.toArray(new String[0]);
                    } else {
                        outFields[i] = rowField;
                    }
                    break;
                default:
                    outFields[i] = rowField;
            }
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(outFields);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema().copy();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
