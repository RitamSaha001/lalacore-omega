import fs from 'node:fs';
import path from 'node:path';

function normalizeString(value) {
  return String(value ?? '').trim();
}

function normalizeBoolean(value) {
  return value === true;
}

function buildYouTubeWatchUrl(videoId) {
  const normalized = normalizeString(videoId);
  return normalized ? `https://www.youtube.com/watch?v=${normalized}` : '';
}

function buildYouTubeEmbedUrl(videoId) {
  const normalized = normalizeString(videoId);
  return normalized
    ? `https://www.youtube.com/embed/${normalized}?playsinline=1&rel=0&modestbranding=1&controls=0&enablejsapi=1`
    : '';
}

function parseSize(value) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function safeJoinUrl(baseUrl, rawPath) {
  const normalizedBase = normalizeString(baseUrl);
  const normalizedPath = normalizeString(rawPath).replace(/^\/+/, '');
  if (!normalizedBase || !normalizedPath) {
    return '';
  }
  try {
    const base = normalizedBase.endsWith('/')
      ? normalizedBase
      : `${normalizedBase}/`;
    return new URL(normalizedPath, base).toString();
  } catch (_) {
    return '';
  }
}

function inferContentType(location) {
  const normalized = normalizeString(location).toLowerCase();
  if (normalized.endsWith('.webm')) {
    return 'video/webm';
  }
  if (normalized.endsWith('.mov')) {
    return 'video/quicktime';
  }
  return 'video/mp4';
}

function buildVideoTitle({ classId, session }) {
  const sessionTitle = normalizeString(session?.title);
  const subject = normalizeString(session?.subject);
  const teacherName = normalizeString(session?.teacherName);
  const pieces = [
    sessionTitle || `Live Class ${normalizeString(classId) || 'Replay'}`,
    subject,
    teacherName ? `by ${teacherName}` : '',
  ].filter(Boolean);
  return pieces.join(' • ').slice(0, 100) || 'Lalacore Live Class Replay';
}

function buildVideoDescription({ classId, session, transcript }) {
  const lines = [
    `Class ID: ${normalizeString(classId) || 'unknown'}`,
    normalizeString(session?.title)
      ? `Title: ${normalizeString(session.title)}`
      : '',
    normalizeString(session?.teacherName)
      ? `Teacher: ${normalizeString(session.teacherName)}`
      : '',
    normalizeString(session?.subject)
      ? `Subject: ${normalizeString(session.subject)}`
      : '',
    normalizeString(session?.topic)
      ? `Topic: ${normalizeString(session.topic)}`
      : '',
  ].filter(Boolean);

  const preview = Array.isArray(transcript)
    ? transcript
        .map((item) => normalizeString(item?.message))
        .filter(Boolean)
        .join(' ')
        .slice(0, 400)
    : '';
  if (preview) {
    lines.push('', 'Transcript preview:', preview);
  }
  return lines.join('\n').slice(0, 5000);
}

function buildTags(configTags, session) {
  const rawTags = [
    ...(normalizeString(configTags)
      ? normalizeString(configTags)
          .split(',')
          .map((item) => item.trim())
      : []),
    normalizeString(session?.subject),
    normalizeString(session?.topic),
    normalizeString(session?.title),
    'lalacore',
    'live class',
  ]
    .map((item) => item.trim())
    .filter(Boolean);
  return [...new Set(rawTags)].slice(0, 20);
}

export class YouTubeUploadService {
  constructor({ config, logger, fetchImpl = fetch }) {
    this.config = config;
    this.logger = logger;
    this.fetch = fetchImpl;
  }

  isEnabled() {
    return this.config.youtube.enabled === true;
  }

  isRequired() {
    return this.config.youtube.required === true;
  }

  isConfigured() {
    if (!this.isEnabled()) {
      return false;
    }
    return Boolean(
      normalizeString(this.config.youtube.accessToken) ||
        (
          normalizeString(this.config.youtube.clientId) &&
          normalizeString(this.config.youtube.clientSecret) &&
          normalizeString(this.config.youtube.refreshToken)
        ),
    );
  }

  async maybeUpload({ classId, session, rawRecordingPath, transcript = [] }) {
    if (!this.isConfigured()) {
      return null;
    }
    return this.upload({
      classId,
      session,
      rawRecordingPath,
      transcript,
    });
  }

  async upload({ classId, session, rawRecordingPath, transcript = [] }) {
    const source = await this.#openRecordingSource(rawRecordingPath);
    const accessToken = await this.#getAccessToken();
    const title = buildVideoTitle({ classId, session });
    const description = buildVideoDescription({ classId, session, transcript });
    const tags = buildTags(this.config.youtube.defaultTags, session);
    const metadata = {
      source_recording_path: normalizeString(rawRecordingPath),
      source_recording_url: source.sourceUrl,
      source_kind: source.kind,
      uploaded_at: new Date().toISOString(),
      privacy_status: this.config.youtube.privacyStatus,
    };

    const resumableUrl = await this.#createResumableUploadSession({
      accessToken,
      source,
      title,
      description,
      tags,
    });
    const response = await this.fetch(resumableUrl, {
      method: 'PUT',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': source.contentType,
        ...(source.contentLength
          ? { 'content-length': String(source.contentLength) }
          : {}),
      },
      body: source.body,
      duplex: 'half',
    });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `YouTube upload failed: ${response.status} ${body || response.statusText}`,
      );
    }

    const payload = await response.json();
    const videoId = normalizeString(payload?.id ?? payload?.videoId);
    if (!videoId) {
      throw new Error('YouTube upload completed without a video id.');
    }

    return {
      playbackProvider: 'youtube',
      videoId,
      embedUrl: buildYouTubeEmbedUrl(videoId),
      recordingUrl: buildYouTubeWatchUrl(videoId),
      title,
      description,
      metadata,
    };
  }

  async #getAccessToken() {
    const direct = normalizeString(this.config.youtube.accessToken);
    if (direct) {
      return direct;
    }

    const clientId = normalizeString(this.config.youtube.clientId);
    const clientSecret = normalizeString(this.config.youtube.clientSecret);
    const refreshToken = normalizeString(this.config.youtube.refreshToken);
    if (!clientId || !clientSecret || !refreshToken) {
      throw new Error(
        'YouTube upload is enabled but no OAuth credentials are configured.',
      );
    }

    const response = await this.fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        refresh_token: refreshToken,
        grant_type: 'refresh_token',
      }),
    });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `YouTube OAuth token refresh failed: ${response.status} ${body || response.statusText}`,
      );
    }
    const payload = await response.json();
    const accessToken = normalizeString(payload?.access_token);
    if (!accessToken) {
      throw new Error('YouTube OAuth token refresh returned no access token.');
    }
    return accessToken;
  }

  async #createResumableUploadSession({
    accessToken,
    source,
    title,
    description,
    tags,
  }) {
    const uploadUrl = new URL(
      'https://www.googleapis.com/upload/youtube/v3/videos',
    );
    uploadUrl.searchParams.set('uploadType', 'resumable');
    uploadUrl.searchParams.set('part', 'snippet,status');
    uploadUrl.searchParams.set(
      'notifySubscribers',
      normalizeBoolean(this.config.youtube.notifySubscribers) ? 'true' : 'false',
    );
    const response = await this.fetch(
      uploadUrl,
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${accessToken}`,
          'content-type': 'application/json; charset=UTF-8',
          'x-upload-content-type': source.contentType,
          ...(source.contentLength
            ? { 'x-upload-content-length': String(source.contentLength) }
            : {}),
        },
        body: JSON.stringify({
          snippet: {
            title,
            description,
            categoryId: this.config.youtube.categoryId,
            tags,
          },
          status: {
            privacyStatus: this.config.youtube.privacyStatus,
            selfDeclaredMadeForKids: normalizeBoolean(
              this.config.youtube.madeForKids,
            ),
            embeddable: true,
            license: 'youtube',
            publicStatsViewable: true,
          },
        }),
      },
    );
    const location = normalizeString(response.headers.get('location'));
    if (!response.ok || !location) {
      const body = await response.text();
      throw new Error(
        `YouTube resumable session failed: ${response.status} ${body || response.statusText}`,
      );
    }
    return location;
  }

  async #openRecordingSource(rawRecordingPath) {
    const normalizedPath = normalizeString(rawRecordingPath);
    if (!normalizedPath) {
      throw new Error('Recording path is empty.');
    }

    if (/^https?:\/\//i.test(normalizedPath)) {
      return this.#openRemoteSource(normalizedPath, 'remote_url');
    }

    const workdir =
      normalizeString(this.config.youtube.recordingWorkdir) || process.cwd();
    const resolvedPath = path.isAbsolute(normalizedPath)
      ? normalizedPath
      : path.resolve(workdir, normalizedPath);
    if (fs.existsSync(resolvedPath)) {
      const stats = await fs.promises.stat(resolvedPath);
      return {
        kind: 'local_file',
        sourceUrl: '',
        contentType: inferContentType(resolvedPath),
        contentLength: stats.size,
        body: fs.createReadStream(resolvedPath),
      };
    }

    const publicUrl = safeJoinUrl(
      this.config.youtube.recordingPublicBaseUrl,
      normalizedPath,
    );
    if (publicUrl) {
      return this.#openRemoteSource(publicUrl, 'public_base_url');
    }

    throw new Error(
      `Recording source is not reachable: ${normalizedPath}. Set RECORDING_PUBLIC_BASE_URL or RECORDING_WORKDIR so the worker can read the mp4.`,
    );
  }

  async #openRemoteSource(url, kind) {
    const response = await this.fetch(url);
    if (!response.ok || !response.body) {
      const body = await response.text();
      throw new Error(
        `Recording download failed: ${response.status} ${body || response.statusText}`,
      );
    }
    return {
      kind,
      sourceUrl: url,
      contentType:
        normalizeString(response.headers.get('content-type')).split(';')[0] ||
        inferContentType(url),
      contentLength: parseSize(response.headers.get('content-length')),
      body: response.body,
    };
  }
}
