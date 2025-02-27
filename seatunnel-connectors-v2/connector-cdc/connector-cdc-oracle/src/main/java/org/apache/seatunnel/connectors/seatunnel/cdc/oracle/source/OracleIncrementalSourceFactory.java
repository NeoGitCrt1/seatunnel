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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@AutoService(Factory.class)
public class OracleIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return OracleIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(JdbcSourceOptions.USERNAME, JdbcSourceOptions.PASSWORD)
                .exclusive(CatalogOptions.TABLE_NAMES, CatalogOptions.TABLE_PATTERN)
                .bundled(JdbcSourceOptions.HOSTNAME, JdbcSourceOptions.PORT)
                .optional(
                        JdbcCatalogOptions.BASE_URL,
                        JdbcSourceOptions.DATABASE_NAMES,
                        OracleSourceOptions.SCHEMA_NAMES,
                        OracleSourceOptions.USE_SELECT_COUNT,
                        OracleSourceOptions.SKIP_ANALYZE,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        JdbcSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        JdbcSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        JdbcSourceOptions.TABLE_NAMES_CONFIG)
                .optional(OracleSourceOptions.STARTUP_MODE, OracleSourceOptions.STOP_MODE)
                .conditional(
                        OracleSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        OracleSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        OracleSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .conditional(
                        OracleSourceOptions.STOP_MODE,
                        StopMode.TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .conditional(
                        OracleSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OracleIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig options = context.getOptions();
            List<CatalogTable> catalogTables = new ArrayList<>();
            try (OracleCatalog catalog =
                    new OracleCatalog(
                            OracleIncrementalSource.IDENTIFIER,
                            options.get(JdbcCatalogOptions.USERNAME),
                            options.get(JdbcCatalogOptions.PASSWORD),
                            OracleURLParser.parse(options.get(JdbcCatalogOptions.BASE_URL)),
                            options.get(JdbcCatalogOptions.SCHEMA))) {
                catalog.open();
                List<CatalogTable> tables = catalog.getTables(options);
                if (tables.isEmpty()) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Can not find catalog table in [%s]",
                                    OracleIncrementalSource.IDENTIFIER));
                }
                catalogTables.addAll(tables);
            }

            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    options.getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables, tableConfigs.get(), s -> TablePath.of(s, true));
            }
            SeaTunnelDataType<SeaTunnelRow> dataType =
                    CatalogTableUtil.convertToMultipleRowType(catalogTables);
            return new OracleIncrementalSource(options, dataType, catalogTables);
        };
    }
}
