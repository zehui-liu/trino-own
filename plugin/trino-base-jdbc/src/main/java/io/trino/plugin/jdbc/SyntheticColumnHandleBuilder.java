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

import java.util.OptionalInt;

import static com.google.common.base.Splitter.fixedLength;
import static com.google.common.base.Verify.verify;

public class SyntheticColumnHandleBuilder
{
    private static final String SEPARATOR = "_";
    private static final int SEPARATOR_LENGTH = SEPARATOR.length();

    public JdbcColumnHandle get(JdbcColumnHandle column, int nextSyntheticColumnId, OptionalInt optionalMaxColumnNameLength)
    {
        if (optionalMaxColumnNameLength.isEmpty()) {
            return JdbcColumnHandle.builderFrom(column)
                    .setColumnName(column.getColumnName() + SEPARATOR + nextSyntheticColumnId)
                    .build();
        }

        int maxColumnNameLength = optionalMaxColumnNameLength.getAsInt();
        int nextSyntheticColumnIdLength = String.valueOf(nextSyntheticColumnId).length();
        verify(maxColumnNameLength >= nextSyntheticColumnIdLength, "Maximum allowed column name length is %s but next synthetic id has length %s", maxColumnNameLength, nextSyntheticColumnIdLength);

        if (nextSyntheticColumnIdLength == maxColumnNameLength) {
            return JdbcColumnHandle.builderFrom(column)
                    .setColumnName(String.valueOf(nextSyntheticColumnId))
                    .build();
        }

        if (nextSyntheticColumnIdLength + SEPARATOR_LENGTH == maxColumnNameLength) {
            return JdbcColumnHandle.builderFrom(column)
                    .setColumnName(SEPARATOR + nextSyntheticColumnId)
                    .build();
        }

        String truncatedColumnName = fixedLength(maxColumnNameLength - SEPARATOR_LENGTH - nextSyntheticColumnIdLength)
                    .split(column.getColumnName())
                    .iterator()
                    .next();
        return JdbcColumnHandle.builderFrom(column)
                .setColumnName(truncatedColumnName + SEPARATOR + nextSyntheticColumnId)
                .build();
    }
}
