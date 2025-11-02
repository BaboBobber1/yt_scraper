import { useMemo, useState } from 'react';

export interface ChannelRecord {
  channel_id: string;
  channel_name: string;
  channel_url: string;
  subscribers: number | null;
  detected_language: string | null;
  lang_confidence: number | null;
  emails: string | null;
  sampled_videos: number | null;
  last_updated: number | null;
}

type SortKey = keyof Pick<ChannelRecord, 'channel_name' | 'subscribers' | 'detected_language' | 'last_updated'>;

type SortState = {
  key: SortKey;
  direction: 'asc' | 'desc';
};

interface DataTableProps {
  channels: ChannelRecord[];
}

const numberFormatter = new Intl.NumberFormat();

function sortChannels(channels: ChannelRecord[], sort: SortState): ChannelRecord[] {
  const sorted = [...channels];
  sorted.sort((a, b) => {
    const aValue = a[sort.key];
    const bValue = b[sort.key];
    if (aValue === bValue) return 0;
    if (aValue === null || aValue === undefined) return sort.direction === 'asc' ? -1 : 1;
    if (bValue === null || bValue === undefined) return sort.direction === 'asc' ? 1 : -1;
    if (typeof aValue === 'number' && typeof bValue === 'number') {
      return sort.direction === 'asc' ? aValue - bValue : bValue - aValue;
    }
    const aText = String(aValue).toLowerCase();
    const bText = String(bValue).toLowerCase();
    return sort.direction === 'asc' ? aText.localeCompare(bText) : bText.localeCompare(aText);
  });
  return sorted;
}

export default function DataTable({ channels }: DataTableProps) {
  const [sort, setSort] = useState<SortState>({ key: 'channel_name', direction: 'asc' });

  const sortedChannels = useMemo(() => sortChannels(channels, sort), [channels, sort]);

  const toggleSort = (key: SortKey) => {
    setSort((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === 'asc' ? 'desc' : 'asc',
        };
      }
      return { key, direction: 'asc' };
    });
  };

  return (
    <section className="bg-surface rounded-2xl p-6 shadow-lg shadow-surface/40">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-medium">Channels ({sortedChannels.length})</h2>
        <p className="text-sm text-slate-400">
          Sorted by {sort.key.replace('_', ' ')} ({sort.direction}).
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-700">
          <thead className="bg-background/60">
            <tr>
              <TableHeader label="Channel" onClick={() => toggleSort('channel_name')} />
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wide text-slate-400">
                URL
              </th>
              <TableHeader label="Subscribers" onClick={() => toggleSort('subscribers')} />
              <TableHeader label="Language" onClick={() => toggleSort('detected_language')} />
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wide text-slate-400">
                Emails
              </th>
              <TableHeader label="Updated" onClick={() => toggleSort('last_updated')} />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {sortedChannels.map((channel) => (
              <tr key={channel.channel_id} className="hover:bg-background/40">
                <td className="px-4 py-3 text-sm font-medium text-slate-100">
                  {channel.channel_name}
                </td>
                <td className="px-4 py-3 text-sm">
                  <a
                    href={channel.channel_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-accent hover:underline"
                  >
                    {channel.channel_url}
                  </a>
                </td>
                <td className="px-4 py-3 text-sm text-slate-200">
                  {typeof channel.subscribers === 'number'
                    ? numberFormatter.format(channel.subscribers)
                    : '—'}
                </td>
                <td className="px-4 py-3 text-sm text-slate-200">
                  {channel.detected_language ?? '—'}
                  {channel.lang_confidence != null && (
                    <span className="ml-2 text-xs text-slate-400">
                      ({(channel.lang_confidence * 100).toFixed(1)}%)
                    </span>
                  )}
                </td>
                <td className="px-4 py-3 text-sm text-slate-200">
                  {channel.emails ? channel.emails.split(',').join(', ') : '—'}
                </td>
                <td className="px-4 py-3 text-sm text-slate-200">
                  {channel.last_updated
                    ? new Date(channel.last_updated * 1000).toLocaleString()
                    : '—'}
                </td>
              </tr>
            ))}
            {sortedChannels.length === 0 && (
              <tr>
                <td
                  colSpan={6}
                  className="px-4 py-8 text-center text-sm text-slate-500"
                >
                  No channels yet. Discover channels to begin.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

interface HeaderProps {
  label: string;
  onClick: () => void;
}

function TableHeader({ label, onClick }: HeaderProps) {
  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wide text-slate-400 cursor-pointer select-none"
      onClick={onClick}
    >
      {label}
    </th>
  );
}
