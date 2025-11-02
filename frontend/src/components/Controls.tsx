import { ChangeEvent } from 'react';

interface ControlsProps {
  keywords: string;
  perKeyword: number;
  onKeywordsChange: (value: string) => void;
  onPerKeywordChange: (value: number) => void;
  onDiscover: () => Promise<void> | void;
  onEnrich: () => Promise<void> | void;
  onExport: () => void;
  loadingDiscover: boolean;
  loadingEnrich: boolean;
  onSearchChange: (value: string) => void;
}

export default function Controls({
  keywords,
  perKeyword,
  onKeywordsChange,
  onPerKeywordChange,
  onDiscover,
  onEnrich,
  onExport,
  loadingDiscover,
  loadingEnrich,
  onSearchChange,
}: ControlsProps) {
  const handlePerKeywordChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = Number(event.target.value);
    if (!Number.isNaN(value)) {
      onPerKeywordChange(value);
    }
  };

  return (
    <section className="bg-surface rounded-2xl p-6 shadow-lg shadow-surface/40 space-y-4">
      <div className="grid gap-4 md:grid-cols-2">
        <label className="flex flex-col gap-2 text-sm text-slate-300">
          Keywords
          <textarea
            value={keywords}
            onChange={(event) => onKeywordsChange(event.target.value)}
            className="min-h-[120px] rounded-xl bg-background/80 border border-slate-700 px-4 py-3 text-slate-100 focus:outline-none focus:ring-2 focus:ring-accent/60"
          />
        </label>
        <div className="flex flex-col gap-4">
          <label className="flex flex-col gap-2 text-sm text-slate-300">
            Results per keyword
            <input
              type="number"
              min={1}
              max={200}
              value={perKeyword}
              onChange={handlePerKeywordChange}
              className="rounded-xl bg-background/80 border border-slate-700 px-4 py-3 text-slate-100 focus:outline-none focus:ring-2 focus:ring-accent/60"
            />
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-300">
            Search table
            <input
              type="search"
              placeholder="Filter by name or email"
              onChange={(event) => onSearchChange(event.target.value)}
              className="rounded-xl bg-background/80 border border-slate-700 px-4 py-3 text-slate-100 focus:outline-none focus:ring-2 focus:ring-accent/60"
            />
          </label>
        </div>
      </div>
      <div className="flex flex-wrap gap-3">
        <button
          onClick={() => {
            void onDiscover();
          }}
          disabled={loadingDiscover}
          className="inline-flex items-center justify-center rounded-xl bg-accent/90 px-5 py-2 text-sm font-medium text-slate-900 transition hover:bg-accent disabled:opacity-50"
        >
          {loadingDiscover ? 'Discovering…' : 'Discover Channels'}
        </button>
        <button
          onClick={() => {
            void onEnrich();
          }}
          disabled={loadingEnrich}
          className="inline-flex items-center justify-center rounded-xl border border-slate-600 px-5 py-2 text-sm font-medium text-slate-200 transition hover:border-accent hover:text-accent disabled:opacity-50"
        >
          {loadingEnrich ? 'Enriching…' : 'Enrich Channels'}
        </button>
        <button
          onClick={onExport}
          className="inline-flex items-center justify-center rounded-xl border border-slate-600 px-5 py-2 text-sm font-medium text-slate-200 transition hover:border-accent hover:text-accent"
        >
          Export CSV (Google Sheets)
        </button>
      </div>
    </section>
  );
}
