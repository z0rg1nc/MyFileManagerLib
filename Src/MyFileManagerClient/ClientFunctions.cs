using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BtmI2p.MiscUtils;

namespace BtmI2p.MyFileManager
{
    public static class MyFileManagerCommonConstants
    {
        public static readonly int ChunkSize = 50000;
    }

    public class FileManagerDataChunk
    {
        public int ChunkNum;
        public byte[] Data = null;
        public byte[] DataHash;
        public int DataLength;

        public FileManagerDataChunk HashOnly()
        {
            return new FileManagerDataChunk()
            {
                ChunkNum = ChunkNum,
                Data = null,
                DataHash = DataHash,
                DataLength = DataLength
            };
        }
    }

    public class FileManagerDataInfo
    {
        public List<FileManagerDataChunk> Chunks;
        public Guid FileGuid;
    }
    public class DownloadFileProgressInfo
    {
        public int TotalSize;
        public int DownloadedSize;
        public int TotalChunkCount;
        public int ChunkDownloadedCount;
    }

    public enum EDownloadErrCodes
    {
        
    }
    public static class MyFileManagerClientFunctions
    {
        public async static Task<byte[]> Download(
            FileManagerDataInfo fileManagerDataInfo,
            Func<int, Task<FileManagerDataChunk>> getChunkFunc,
            CancellationToken token,
            int windowSize,
            Subject<DownloadFileProgressInfo> progressSubject = null
        )
        {
            if(fileManagerDataInfo == null)
                throw new ArgumentNullException(
                    MyNameof.GetLocalVarName(() => fileManagerDataInfo)
                );
            var lockSem = new SemaphoreSlim(windowSize);
            var tasks = new Task[fileManagerDataInfo.Chunks.Count];
            int downloadedSize = 0;
            int totalSize = fileManagerDataInfo.Chunks.Select(x => x.DataLength).Sum();
            int totalChunkCount = fileManagerDataInfo.Chunks.Count;
            int downloadedChunkCount = 0;
            for (int i = 0; i < fileManagerDataInfo.Chunks.Count; i++)
            {
                int j = i;
                tasks[j] = await Task.Factory.StartNew(async () =>
                {
                    using (await lockSem.GetDisposable().ConfigureAwait(false))
                    {
                        FileManagerDataChunk chunk;
                        while (true)
                        {
                            try
                            {
                                chunk = await getChunkFunc(j)
                                    .ThrowIfCancelled(token).ConfigureAwait(false);
                                break;
                            }
                            catch (TimeoutException)
                            {
                            }
                        }
                        using (var hash = new SHA256Managed())
                        {
                            if(chunk.Data == null)
                                throw new ArgumentNullException(
                                    chunk.MyNameOfProperty(e => e.Data)
                                );
                            if(chunk.Data.Length != fileManagerDataInfo.Chunks[j].DataLength)
                                throw new ArgumentOutOfRangeException(
                                    chunk.Data.MyNameOfProperty(e => e.Length)
                                );
                            if(
                                !hash.ComputeHash(chunk.Data)
                                    .SequenceEqual(fileManagerDataInfo.Chunks[j].DataHash)
                            )
                                throw new Exception("Wrong hash");
                            fileManagerDataInfo.Chunks[j].Data = chunk.Data;
                            var totalDownloadedSize = Interlocked.Add(
                                ref downloadedSize, 
                                chunk.DataLength
                            );
                            var tDownloadedChunkCount = Interlocked.Increment(
                                ref downloadedChunkCount
                            );
                            if(progressSubject != null)
                                progressSubject.OnNext(
                                    new DownloadFileProgressInfo()
                                    {
                                        DownloadedSize = totalDownloadedSize,
                                        TotalSize = totalSize,
                                        TotalChunkCount = totalChunkCount,
                                        ChunkDownloadedCount = tDownloadedChunkCount
                                    }
                                );
                        }
                    }
                }).ConfigureAwait(false);
            }
            foreach (var task in tasks)
            {
                await task.ConfigureAwait(false);
            }
            return fileManagerDataInfo.Chunks.OrderBy(x => x.ChunkNum)
                .SelectMany(x => x.Data).ToArray();
        }
    }
}
