import axios from 'axios';
import { ChannelRecord } from '../components/DataTable';

const client = axios.create({
  baseURL: '/api',
});

export async function discoverChannels(payload: {
  keywords?: string[];
  perKeyword?: number;
}) {
  await client.post('/discover', payload);
}

export async function enrichChannels(payload?: { limit?: number }) {
  await client.post('/enrich', payload ?? {});
}

export async function fetchChannels(): Promise<ChannelRecord[]> {
  const response = await client.get<ChannelRecord[]>('/channels', {
    params: { limit: 500 },
  });
  return response.data;
}
