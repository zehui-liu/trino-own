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

package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.model.HoodieBaseFile;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.Strings.isNullOrEmpty;


/*
 * @author：ze hui
 * @date：2023/9/19
 */
public class HudiFile
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;

    @JsonCreator
    public HudiFile(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime)
    {
        checkArgument(!isNullOrEmpty(path), "path is null or empty");
        checkArgument(start >= 0, "start should not be negative");
        checkArgument(length >= 0, "length should not be negative");
        checkArgument(start + length <= fileSize, "fileSize must be at least start + length");

        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiFile hudiFile = (HudiFile) o;
        return Objects.equals(path, hudiFile.path) &&
                start == hudiFile.start &&
                length == hudiFile.length &&
                fileSize == hudiFile.fileSize &&
                fileModifiedTime == hudiFile.fileModifiedTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length, fileSize, fileModifiedTime);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("start", start)
                .add("length", length)
                .add("fileSize", fileSize)
                .add("fileModifiedTime", fileModifiedTime)
                .toString();
    }

    public static HudiFile fromFileStatus(FileStatus fileStatus)
    {
        return new HudiFile(fileStatus.getPath().toString(), 0, fileStatus.getLen(), fileStatus.getLen(), fileStatus.getModificationTime());
    }

    public static HudiFile fromHoodieBaseFile(HoodieBaseFile baseFile)
    {
        return new HudiFile(baseFile.getPath(), 0, baseFile.getFileLen(), baseFile.getFileLen(), baseFile.getFileStatus().getModificationTime());
    }
}
