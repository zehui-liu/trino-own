/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import com.google.common.base.VerifyException;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSyntheticColumnHandleBuilder
{
    private final SyntheticColumnHandleBuilder syntheticColumnHandleBuilder = new SyntheticColumnHandleBuilder();

    @Test
    public void testShortColumnName()
    {
        int maximumLength = 30;
        String columnName = "a".repeat(maximumLength - 4);

        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName(columnName)
                .build();

        JdbcColumnHandle result = syntheticColumnHandleBuilder.get(column, 123, OptionalInt.of(maximumLength));
        assertThat(result.getColumnName()).isEqualTo(columnName + "_123");
    }

    @Test
    public void testColumnNameExceedsLength()
    {
        int maximumLength = 30;
        String columnName = "a".repeat(maximumLength);

        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName(columnName)
                .build();

        JdbcColumnHandle result = syntheticColumnHandleBuilder.get(column, 123, OptionalInt.of(maximumLength));
        assertThat(result.getColumnName()).isEqualTo("a".repeat(maximumLength - 4) + "_123");
    }

    @Test
    public void testSyntheticIdExceedsLength()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("a")
                .build();

        assertThatThrownBy(() -> syntheticColumnHandleBuilder.get(column, 1234, OptionalInt.of(3)))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Maximum allowed column name length is 3 but next synthetic id has length 4");
    }

    @Test
    public void testSyntheticIdEqualsLength()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("a")
                .build();

        JdbcColumnHandle result = syntheticColumnHandleBuilder.get(column, 1234, OptionalInt.of(4));
        assertThat(result.getColumnName()).isEqualTo("1234");
    }

    @Test
    public void testSyntheticIdWithSeparatorEqualsLength()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("a")
                .build();

        JdbcColumnHandle result = syntheticColumnHandleBuilder.get(column, 1234, OptionalInt.of(5));
        assertThat(result.getColumnName()).isEqualTo("_1234");
    }

    private static JdbcColumnHandle.Builder getDefaultColumnHandleBuilder()
    {
        return JdbcColumnHandle.builder()
                .setJdbcTypeHandle(JDBC_VARCHAR)
                .setColumnType(VARCHAR);
    }
}
