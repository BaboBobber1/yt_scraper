import { useCallback, useEffect, useMemo, useState } from 'react';
import Controls from './components/Controls';
import Progress from './components/Progress';
import DataTable, { ChannelRecord } from './components/DataTable';
import { discoverChannels, enrichChannels, fetchChannels } from './lib/api';

const DEFAULT_KEYWORDS = [
  'crypto',
  'bitcoin',
  'ethereum',
  'defi',
  'altcoin',
  'memecoin',
  'onchain',
  'crypto trading',
];

function parseKeywords(input: string): string[] {
  return input
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

const DEFAULT_KEYWORD_TEXT = DEFAULT_KEYWORDS.join(', ');

export default function App() {
  const [keywordsInput, setKeywordsInput] = useState<string>(DEFAULT_KEYWORD_TEXT);
  const [perKeyword, setPerKeyword] = useState<number>(100);
  const [loadingDiscover, setLoadingDiscover] = useState(false);
  const [loadingEnrich, setLoadingEnrich] = useState(false);
  const [channels, setChannels] = useState<ChannelRecord[]>([]);
  const [searchTerm, setSearchTerm] = useState('');

  const loadChannels = useCallback(async () => {
    const data = await fetchChannels();
    setChannels(data);
  }, []);

  useEffect(() => {
    loadChannels();
  }, [loadChannels]);

  const handleDiscover = useCallback(async () => {
    setLoadingDiscover(true);
    try {
      const keywords = parseKeywords(keywordsInput);
      await discoverChannels({ keywords, perKeyword });
      await loadChannels();
    } finally {
      setLoadingDiscover(false);
    }
  }, [keywordsInput, perKeyword, loadChannels]);

  const handleEnrich = useCallback(async () => {
    setLoadingEnrich(true);
    try {
      await enrichChannels();
      await loadChannels();
    } finally {
      setLoadingEnrich(false);
    }
  }, [loadChannels]);

  const handleExport = useCallback(() => {
    window.open('/api/export/csv', '_blank');
  }, []);

  const filteredChannels = useMemo(() => {
    if (!searchTerm) return channels;
    const term = searchTerm.toLowerCase();
    return channels.filter(
      (channel) =>
        channel.channel_name.toLowerCase().includes(term) ||
        (channel.emails ?? '').toLowerCase().includes(term)
    );
  }, [channels, searchTerm]);

  return (
    <div className="min-h-screen bg-background text-slate-100">
      <div className="max-w-6xl mx-auto py-10 px-4 space-y-8">
        <header className="flex flex-col gap-2">
          <h1 className="text-3xl font-semibold">Crypto YouTube Harvester</h1>
          <p className="text-slate-400">
            Discover, enrich, and export public crypto YouTube channels. Runs locally in
            dark mode.
          </p>
        </header>
        <Controls
          keywords={keywordsInput}
          perKeyword={perKeyword}
          onKeywordsChange={setKeywordsInput}
          onPerKeywordChange={setPerKeyword}
          onDiscover={handleDiscover}
          onEnrich={handleEnrich}
          onExport={handleExport}
          loadingDiscover={loadingDiscover}
          loadingEnrich={loadingEnrich}
          onSearchChange={setSearchTerm}
        />
        <Progress />
        <DataTable channels={filteredChannels} />
      </div>
    </div>
  );
}
