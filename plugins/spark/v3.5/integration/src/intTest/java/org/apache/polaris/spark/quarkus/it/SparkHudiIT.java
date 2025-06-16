/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.spark.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
public class SparkHudiIT extends SparkIntegrationBase {
    private String defaultNs;
    private String tableRootDir;

    private String getTableLocation(String tableName) {
        return String.format("%s/%s", tableRootDir, tableName);
    }

    private String getTableNameWithRandomSuffix() {
        return generateName("huditb");
    }

    @BeforeEach
    public void createDefaultResources(@TempDir Path tempDir) {
        spark.sparkContext().setLogLevel("WARN");
        defaultNs = generateName("hudi");
        // create a default namespace
        sql("CREATE NAMESPACE %s", defaultNs);
        sql("USE NAMESPACE %s", defaultNs);
        tableRootDir =
                IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(defaultNs).getPath();
    }

    @AfterEach
    public void cleanupHudiData() {
        // clean up hudi data
        File dirToDelete = new File(tableRootDir);
        FileUtils.deleteQuietly(dirToDelete);
        sql("DROP NAMESPACE %s", defaultNs);
    }

    @Test
    public void testBasicTableOperations() {
        // create a regular hudi table
        String huditb1 = "huditb1";
        sql(
                "CREATE TABLE %s (id INT, name STRING) USING HUDI LOCATION '%s'",
                huditb1, getTableLocation(huditb1));
        sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", huditb1);
        List<Object[]> results = sql("SELECT * FROM %s WHERE id > 1 ORDER BY id DESC", huditb1);
        assertThat(results.size()).isEqualTo(1);
        assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

        // create a hudi table with partition
        String huditb2 = "huditb2";
        sql(
                "CREATE TABLE %s (name String, age INT, country STRING) USING HUDI PARTITIONED BY (country) LOCATION '%s'",
                huditb2, getTableLocation(huditb2));
        sql(
                "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
                huditb2);
        results = sql("SELECT name, country FROM %s ORDER BY age", huditb2);
        assertThat(results.size()).isEqualTo(3);
        assertThat(results.get(0)).isEqualTo(new Object[] {"anna", "US"});
        assertThat(results.get(1)).isEqualTo(new Object[] {"yan", "CHINA"});
        assertThat(results.get(2)).isEqualTo(new Object[] {"james", "US"});

        // verify the partition dir is created
        List<String> subDirs = listDirs(getTableLocation(huditb2));
        assertThat(subDirs).contains(".hoodie", "country=CHINA", "country=US");

        // test listTables
        List<Object[]> tables = sql("SHOW TABLES");
        assertThat(tables.size()).isEqualTo(2);
        assertThat(tables)
                .contains(
                        new Object[] {defaultNs, huditb1, false}, new Object[] {defaultNs, huditb2, false});

        sql("DROP TABLE %s", huditb1);
        sql("DROP TABLE %s", huditb2);
        tables = sql("SHOW TABLES");
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    public void testAlterOperations() {
        String huditb = getTableNameWithRandomSuffix();
        sql(
                "CREATE TABLE %s (id INT, name STRING) USING HUDI LOCATION '%s'",
                huditb, getTableLocation(huditb));
        sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", huditb);

        // test alter columns
        // add two new columns to the table
        sql("Alter TABLE %s ADD COLUMNS (city STRING, age INT)", huditb);
        // add one more row to the table
        sql("INSERT INTO %s VALUES (3, 'john', 'SFO', 20)", huditb);
        // verify the table now have 4 columns with correct result
        List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", huditb);
        assertThat(results.size()).isEqualTo(3);
        assertThat(results).contains(new Object[] {1, "anna", null, null});
        assertThat(results).contains(new Object[] {2, "bob", null, null});
        assertThat(results).contains(new Object[] {3, "john", "SFO", 20});

        // drop and rename column require set the hoodie.keep.max.commits property
        sql("ALTER TABLE %s SET TBLPROPERTIES ('hoodie.keep.max.commits' = '50')", huditb);
        // drop column age
        sql("Alter TABLE %s DROP COLUMN age", huditb);
        // verify the table now have 3 columns with correct result
        results = sql("SELECT * FROM %s ORDER BY id", huditb);
        assertThat(results.size()).isEqualTo(3);
        assertThat(results).contains(new Object[] {1, "anna", null});
        assertThat(results).contains(new Object[] {2, "bob", null});
        assertThat(results).contains(new Object[] {3, "john", "SFO"});

        // rename column city to address
        sql("Alter TABLE %s RENAME COLUMN city TO address", huditb);
        // verify column address exists
        results = sql("SELECT id, address FROM %s ORDER BY id", huditb);
        assertThat(results.size()).isEqualTo(3);
        assertThat(results).contains(new Object[] {1, null});
        assertThat(results).contains(new Object[] {2, null});
        assertThat(results).contains(new Object[] {3, "SFO"});

        // test alter properties
        sql(
                "ALTER TABLE %s SET TBLPROPERTIES ('description' = 'people table', 'test-owner' = 'test-user')",
                huditb);
        List<Object[]> tableInfo = sql("DESCRIBE TABLE EXTENDED %s", huditb);
        // find the table properties result
        String properties = null;
        for (Object[] info : tableInfo) {
            if (info[0].equals("Table Properties")) {
                properties = (String) info[1];
                break;
            }
        }
        assertThat(properties).contains("description=people table,test-owner=test-user");
        sql("DROP TABLE %s", huditb);
    }

    @Test
    public void testUnsupportedAlterTableOperations() {
        String huditb = getTableNameWithRandomSuffix();
        sql(
                "CREATE TABLE %s (name String, age INT, country STRING) USING HUDI PARTITIONED BY (country) LOCATION '%s'",
                huditb, getTableLocation(huditb));

        // ALTER TABLE ... RENAME TO ... fails
        assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_hudi", huditb))
                .isInstanceOf(UnsupportedOperationException.class);

        // ALTER TABLE ... SET LOCATION ... fails
        assertThatThrownBy(() -> sql("ALTER TABLE %s SET LOCATION '/tmp/new/path'", huditb))
                .isInstanceOf(AnalysisException.class);

        sql("DROP TABLE %s", huditb);
    }

    @Test
    public void testUnsupportedTableCreateOperations() {
        String huditb = getTableNameWithRandomSuffix();
        // create hudi table with no location
        assertThatThrownBy(() -> sql("CREATE TABLE %s (id INT, name STRING) USING HUDI", huditb))
                .isInstanceOf(UnsupportedOperationException.class);

        // CTAS fails
        assertThatThrownBy(
                () ->
                        sql(
                                "CREATE TABLE %s USING HUDI LOCATION '%s' AS SELECT 1 AS id",
                                huditb, getTableLocation(huditb)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testDataframeSaveOperations() {
        List<Row> data = Arrays.asList(RowFactory.create("Alice", 30), RowFactory.create("Bob", 25));
        StructType schema =
                new StructType(
                        new StructField[] {
                                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
                        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        String huditb = getTableNameWithRandomSuffix();
        // saveAsTable requires support for hudi requires CTAS support for third party catalog
        // in hudi catalog, which is currently not supported.
        assertThatThrownBy(
                () ->
                        df.write()
                                .format("hudi")
                                .option("path", getTableLocation(huditb))
                                .saveAsTable(huditb))
                .isInstanceOf(IllegalArgumentException.class);

        // verify regular dataframe saving still works
        df.write().format("hudi").save(getTableLocation(huditb));

        // verify the partition dir is created
        List<String> subDirs = listDirs(getTableLocation(huditb));
        assertThat(subDirs).contains(".hoodie");

        // verify we can create a table out of the exising hudi location
        sql("CREATE TABLE %s USING HUDI LOCATION '%s'", huditb, getTableLocation(huditb));
        List<Object[]> tables = sql("SHOW TABLES");
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables).contains(new Object[] {defaultNs, huditb, false});

        sql("INSERT INTO %s VALUES ('Anna', 11)", huditb);

        List<Object[]> results = sql("SELECT * FROM %s ORDER BY name", huditb);
        assertThat(results.size()).isEqualTo(3);
        assertThat(results.get(0)).isEqualTo(new Object[] {"Alice", 30});
        assertThat(results.get(1)).isEqualTo(new Object[] {"Anna", 11});
        assertThat(results.get(2)).isEqualTo(new Object[] {"Bob", 25});

        sql("DROP TABLE %s", huditb);
    }
}
