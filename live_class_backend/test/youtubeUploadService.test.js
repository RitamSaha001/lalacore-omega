import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { YouTubeUploadService } from '../src/services/youtubeUploadService.js';

function createLogger() {
  return {
    info() {},
    warn() {},
    error() {},
  };
}

async function drainBody(body) {
  if (!body) {
    return Buffer.alloc(0);
  }
  if (body instanceof Uint8Array) {
    return Buffer.from(body);
  }
  if (typeof body[Symbol.asyncIterator] === 'function') {
    const chunks = [];
    for await (const chunk of body) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }
  if (typeof body.getReader === 'function') {
    const reader = body.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value) {
        chunks.push(Buffer.from(value));
      }
    }
    return Buffer.concat(chunks);
  }
  return Buffer.alloc(0);
}

test('YouTubeUploadService uploads a local recording with refreshed OAuth access', async () => {
  const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'youtube-upload-test-'));
  const recordingPath = path.join(tmpDir, 'recording.mp4');
  writeFileSync(recordingPath, Buffer.from([1, 2, 3, 4]));

  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url) === 'https://oauth2.googleapis.com/token') {
      return new Response(
        JSON.stringify({ access_token: 'fresh-access-token' }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' },
        },
      );
    }
    if (
      String(url).startsWith(
        'https://www.googleapis.com/upload/youtube/v3/videos',
      )
    ) {
      return new Response('', {
        status: 200,
        headers: { location: 'https://upload.youtube.test/session/1' },
      });
    }
    if (String(url) === 'https://upload.youtube.test/session/1') {
      await drainBody(init.body);
      return new Response(JSON.stringify({ id: 'dQw4w9WgXcQ' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`Unexpected fetch call: ${String(url)}`);
  };

  const service = new YouTubeUploadService({
    config: {
      youtube: {
        enabled: true,
        required: true,
        accessToken: '',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        refreshToken: 'refresh-token',
        privacyStatus: 'unlisted',
        categoryId: '27',
        defaultTags: 'jee,physics',
        recordingPublicBaseUrl: '',
        recordingWorkdir: '',
        notifySubscribers: false,
        madeForKids: false,
      },
    },
    logger: createLogger(),
    fetchImpl,
  });

  try {
    const result = await service.upload({
      classId: 'physics_live_01',
      rawRecordingPath: recordingPath,
      session: {
        title: 'Gauss Law',
        teacherName: 'Dr Sharma',
        subject: 'Physics',
        topic: 'Electrostatics',
      },
      transcript: [{ message: 'Gauss law links electric flux and charge.' }],
    });

    assert.equal(result.playbackProvider, 'youtube');
    assert.equal(result.videoId, 'dQw4w9WgXcQ');
    assert.equal(
      result.recordingUrl,
      'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
    );
    assert.match(
      result.embedUrl,
      /https:\/\/www\.youtube\.com\/embed\/dQw4w9WgXcQ/,
    );
    assert.equal(calls[0].url, 'https://oauth2.googleapis.com/token');
    assert.match(
      calls[1].url,
      /https:\/\/www\.googleapis\.com\/upload\/youtube\/v3\/videos/,
    );
    assert.equal(calls[2].url, 'https://upload.youtube.test/session/1');
  } finally {
    rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('YouTubeUploadService resolves a relative recording path through RECORDING_PUBLIC_BASE_URL', async () => {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url) === 'https://cdn.example.com/recordings/class/file.mp4') {
      return new Response(new Uint8Array([1, 2, 3, 4]), {
        status: 200,
        headers: {
          'content-type': 'video/mp4',
          'content-length': '4',
        },
      });
    }
    if (
      String(url).startsWith(
        'https://www.googleapis.com/upload/youtube/v3/videos',
      )
    ) {
      return new Response('', {
        status: 200,
        headers: { location: 'https://upload.youtube.test/session/2' },
      });
    }
    if (String(url) === 'https://upload.youtube.test/session/2') {
      await drainBody(init.body);
      return new Response(JSON.stringify({ id: 'dQw4w9WgXcQ' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`Unexpected fetch call: ${String(url)}`);
  };

  const service = new YouTubeUploadService({
    config: {
      youtube: {
        enabled: true,
        required: false,
        accessToken: 'direct-access-token',
        clientId: '',
        clientSecret: '',
        refreshToken: '',
        privacyStatus: 'private',
        categoryId: '27',
        defaultTags: '',
        recordingPublicBaseUrl: 'https://cdn.example.com',
        recordingWorkdir: '',
        notifySubscribers: false,
        madeForKids: false,
      },
    },
    logger: createLogger(),
    fetchImpl,
  });

  const result = await service.upload({
    classId: 'math_live_01',
    rawRecordingPath: 'recordings/class/file.mp4',
    session: { title: 'Definite Integration' },
    transcript: [],
  });

  assert.equal(result.videoId, 'dQw4w9WgXcQ');
  assert.equal(calls[0].url, 'https://cdn.example.com/recordings/class/file.mp4');
});
