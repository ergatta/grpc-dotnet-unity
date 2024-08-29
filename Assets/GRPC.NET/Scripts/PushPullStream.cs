using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Best.HTTP;
using Best.HTTP.Request.Upload;

namespace GRPC.NET
{
    public class PushPullStream : UploadStreamBase
    {
        private const long MAX_BUFFER_LENGTH = 5 * 1024 * 1024; // 5 MB

        public bool NonBlockingRead = false;

        private readonly Queue<byte> m_Buffer = new();

        private bool m_Flushed;
        private bool m_Closed;

        private Exception m_Exception;

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return 0;

            var readLength = 0;
            lock (m_Buffer)
            {
                while (!ReadAvailable()) Monitor.Wait(m_Buffer);

                for (; readLength < count && m_Buffer.Count > 0; readLength++)
                    buffer[offset + readLength] = m_Buffer.Dequeue();

                Monitor.Pulse(m_Buffer);
            }

            // BestHTTP expects us to return -1 when we have no data (but have not reached EOF yet).
            if (readLength == 0 && !m_Closed) return -1;

            return readLength;
        }

        private bool ReadAvailable()
        {
            if (m_Exception != null)
                throw m_Exception;

            // Either we have data to read, or we got flushed (e.g. stream got closed)
            // or we are in non blocking read mode.
            return m_Buffer.Count > 0 || m_Flushed || m_Closed || NonBlockingRead;
        }

        // Avoid sending content-length=0 (EOF) by using -2 for chunked encoding.
        public override long Length => -2;

        public override long Position
        {
            get => 0;
            set => throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (m_Buffer)
            {
                while (m_Buffer.Count >= MAX_BUFFER_LENGTH)
                    Monitor.Wait(m_Buffer);

                for (var i = offset; i < offset + count; i++)
                    m_Buffer.Enqueue(buffer[i]);

                m_Flushed = false;
                Monitor.Pulse(m_Buffer);
            }
        }

        public override void Flush()
        {
            lock (m_Buffer)
            {
                m_Flushed = true;
                Monitor.Pulse(m_Buffer);
            }
            Signaler?.SignalThread();
        }

        public override void Close() => CloseWithException(null);

        public void CloseWithException(Exception ex)
        {
            m_Exception = ex;
            m_Closed = true;
            Flush();
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        public override void BeforeSendHeaders(HTTPRequest request) {}

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
    }
}
