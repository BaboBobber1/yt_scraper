import { useEffect, useRef, useState } from 'react';

type ProgressEvent = {
  stage: string;
  status: string;
  keyword?: string;
  channel_id?: string;
  detail?: string;
};

export default function Progress() {
  const [events, setEvents] = useState<ProgressEvent[]>([]);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const source = new EventSource('/api/progress');
    source.onmessage = (message) => {
      try {
        const payload = JSON.parse(message.data);
        if (payload.event === 'keepalive') {
          return;
        }
        setEvents((prev) => [...prev.slice(-200), payload]);
      } catch (error) {
        console.error('Failed to parse progress event', error);
      }
    };
    source.onerror = () => {
      source.close();
    };
    return () => {
      source.close();
    };
  }, []);

  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [events]);

  if (events.length === 0) {
    return (
      <section className="bg-surface rounded-2xl p-6 shadow-lg shadow-surface/40">
        <h2 className="text-lg font-medium mb-2">Progress</h2>
        <p className="text-sm text-slate-400">Actions will appear here.</p>
      </section>
    );
  }

  return (
    <section className="bg-surface rounded-2xl p-6 shadow-lg shadow-surface/40">
      <h2 className="text-lg font-medium mb-2">Progress</h2>
      <div
        ref={containerRef}
        className="max-h-48 overflow-y-auto space-y-2 pr-2 text-sm text-slate-300"
      >
        {events.map((event, index) => (
          <div key={`${event.stage}-${event.status}-${index}`} className="border border-slate-700/60 rounded-lg px-3 py-2">
            <p className="font-medium text-slate-200">
              {event.stage?.toUpperCase()} — {event.status}
            </p>
            {event.keyword && <p className="text-xs text-slate-400">Keyword: {event.keyword}</p>}
            {event.channel_id && (
              <p className="text-xs text-slate-400">Channel ID: {event.channel_id}</p>
            )}
            {event.detail && <p className="text-xs text-rose-400">{event.detail}</p>}
          </div>
        ))}
      </div>
    </section>
  );
}
