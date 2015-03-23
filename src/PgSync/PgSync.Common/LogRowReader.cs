using System;
using System.IO;

namespace PgSync.Common
{
    public class LogRowReader
    {
        public static ushort[] ReadUInt16Array(BinaryReader reader, int arrayLength)
        {
            ushort[] array = new ushort[arrayLength];
            for (int ii = 0; ii < arrayLength; ii++)
            {
                array[ii] = reader.ReadUInt16();
            }

            return array;
        }

        public static void Read(byte[] byteArray, AllocationUnit allocationUnit)
        {
            using (var stream = new MemoryStream(byteArray))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var statusBits = reader.ReadUInt16();
                    var fixedLengthColumnsLength = reader.ReadUInt16();

                    // read the fixed column data - since we want to process this data as a stream,
                    // it is important that we read the data as one big block to advance the stream.
                    // once it has been read into the block, it can be carved into pieces.
                    var fixedLengthColumns = reader.ReadBytes(fixedLengthColumnsLength - 4);

                    var numColumnsInDataRow = reader.ReadUInt16();
                    var numColumnBitmapLength = (numColumnsInDataRow / 8) + (numColumnsInDataRow % 8 == 0 ? 0 : 1);
                    var nullabilityBitmap = reader.ReadBytes(numColumnBitmapLength);

                    // if there are no variable length columns, we're not going to get from
                    // the stream past this point.
                    ushort variableLengthColumnsCount = 0;
                    ushort[] variableLengthColumnsRowOffset = null;

                    try
                    {
                        variableLengthColumnsCount = reader.ReadUInt16();
                        variableLengthColumnsRowOffset = ReadUInt16Array(reader, variableLengthColumnsCount);

                        // the rest of the data in the stream are the variable length data; we assume that
                        // the very next byte is the offset of the first variableLengthColumn.  It allows us
                        // to baseline using the stream rather than using the raw byte array.
                        for (int ii = 0; ii < variableLengthColumnsRowOffset.Length; ii++ )
                        {
                            var offsetX = variableLengthColumnsRowOffset[ii];
                            var offsetY = ii == variableLengthColumnsRowOffset.Length - 1 ? byteArray.Length : variableLengthColumnsRowOffset[ii + 1];
                            var targetLength = offsetY - offsetX;
                            var targetArray = new byte[targetLength];
                            Array.Copy(byteArray, offsetX, targetArray, 0, targetLength);
                        }
                    }
                    catch (EndOfStreamException)
                    {
                    }

                    Console.WriteLine("StatusBits: {0}", statusBits);
                    Console.WriteLine("FixedLengthColumnsLength: {0}", fixedLengthColumnsLength);
                    Console.WriteLine("NumColumnsInDataRow: {0}", numColumnsInDataRow);
                    Console.WriteLine("NumColumnBitmapLength: {0}", numColumnBitmapLength);
                    Console.WriteLine("NumVariableLengthColumns: {0}", variableLengthColumnsCount);
                    Console.WriteLine("VariableLengthColumnsRowOffset: {0}", variableLengthColumnsRowOffset);
                }
            }
        }
    }
}
