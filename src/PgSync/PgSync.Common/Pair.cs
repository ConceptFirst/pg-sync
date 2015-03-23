namespace PgSync.Common
{
    public class Pair<TA,TB>
    {
        public TA A { get; set; }
        public TB B { get; set; }

        public Pair() { }
        public Pair(TA a, TB b)
        {
            A = a;
            B = b;
        }

        public override int GetHashCode()
        {
            return A.GetHashCode() * 397 ^ B.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var pairX = obj as Pair<TA,TB>;
            if (pairX == null)
                return false;

            return
                Equals(A, pairX.A) &&
                Equals(B, pairX.B);
        }
    }
}
