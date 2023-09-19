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
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiFile;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static io.trino.plugin.hudi.HudiUtil.getFileStatus;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.parquet.Strings.isNullOrEmpty;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public Stream<HudiSplit> createSplits(
            String commitTime,
            FileSlice fileSlice,
            List<HivePartitionKey> partitionKeys)
    {
        HudiFile baseFile = fileSlice
                .getBaseFile()
                .map(f -> (isNullOrEmpty(f.getPath()) || 0 == f.getFileLen()) ? null : HudiFile.fromHoodieBaseFile(f))
                .orElse(null);

        if (COPY_ON_WRITE.equals(hudiTableHandle.getTableType())) {
            if (baseFile == null) {
                return Stream.empty();
            }

            return createSplitsFromFileStatus(partitionKeys, getFileStatus(fileSlice.getBaseFile().get()), commitTime);
        }
        else if (MERGE_ON_READ.equals(hudiTableHandle.getTableType())) {
            List<HudiFile> logFiles = fileSlice.getLogFiles()
                    .map(logFile -> new HudiFile(logFile.getPath().toString(), 0, logFile.getFileSize(), logFile.getFileSize(), logFile.getFileStatus().getModificationTime()))
                    .collect(toImmutableList());
            long logFilesSize = logFiles.size() > 0 ? logFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum) : 0L;
            long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

            return Stream.of(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    Optional.ofNullable(baseFile),
                    logFiles,
                    hudiSplitWeightProvider.calculateSplitWeight(sizeInBytes),
                    commitTime));
        }
        else {
            throw new TrinoException(HUDI_UNKNOWN_TABLE_TYPE, "Could not create page source for table type " + hudiTableHandle.getTableType());
        }
    }

    private Stream<HudiSplit> createSplitsFromFileStatus(List<HivePartitionKey> partitionKeys, FileStatus fileStatus, String commitTime)
    {
        List<FileSplit> splits;
        try {
            splits = createSplits(fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, e);
        }

        return splits.stream()
                .map(fileSplit -> new HudiSplit(
                        ImmutableList.of(),
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        Optional.of(HudiFile.fromFileStatus(fileStatus)), // TODO: should use fileSplit?
                        ImmutableList.of(),
                        hudiSplitWeightProvider.calculateSplitWeight(fileSplit.getLength()),
                        commitTime));
    }

    private List<FileSplit> createSplits(FileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        if (length == 0) {
            return ImmutableList.of(new FileSplit(path, 0, 0, new String[0]));
        }

        if (!isSplitable(path)) {
            return ImmutableList.of(new FileSplit(path, 0, length, (String[]) null));
        }

        ImmutableList.Builder<FileSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.getBlockSize();

        long bytesRemaining = length;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            splits.add(new FileSplit(path, length - bytesRemaining, splitSize, (String[]) null));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining != 0) {
            splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, (String[]) null));
        }
        return splits.build();
    }

    private static boolean isSplitable(Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }
}
