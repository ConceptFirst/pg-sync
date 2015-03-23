using System;

namespace PgSync.Common
{
    public class TrackedDisposable : IDisposable
    {
        public Action DisposeEvent { get; set; }

        public void Dispose()
        {
            if (DisposeEvent != null)
            {
                DisposeEvent();
            }
        }

        public TrackedDisposable() { }
        public TrackedDisposable(Action disposeEvent)
        {
            DisposeEvent = disposeEvent;
        }
    }
}
