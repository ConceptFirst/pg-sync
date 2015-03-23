using System.Collections.Generic;

namespace PgSync.Common
{
    public class ForeignReference
    {
        public string SourceOID { get; set; }
        public string SourceSchema { get; set; }
        public string SourceTable { get; set; }
        public string SourceFullyQualifiedName
        {
            get { return SourceSchema + "." + SourceTable; }
        }

        public string ReferenceName { get; set; }
        public string ReferenceTable { get; set; }
        public IList<string> ReferenceColumns { get; set; }
    }
}
