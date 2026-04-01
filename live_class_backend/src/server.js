import http from 'node:http';

import { createBackendApp } from './app.js';
import { config } from './config/env.js';
import { startTelemetry } from './observability/telemetry.js';
import { createLogger } from './utils/logger.js';

const logger = createLogger('server');

try {
  const telemetry = await startTelemetry({
    config,
    logger: createLogger('telemetry'),
  });
  const runtime = await createBackendApp();
  const server = http.createServer(runtime.app);

  await runtime.redisPubSub.connect();
  runtime.classroomHub.bindServer(server);

  server.listen(config.port, () => {
    logger.info('server_listening', {
      port: config.port,
      nodeId: config.nodeId,
    });
  });

  let shuttingDown = false;

  async function shutdown(signal) {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    logger.info('shutdown_started', {
      signal,
    });

    const serverClosed = new Promise((resolve) => {
      server.close(() => {
        resolve();
      });
    });

    await runtime.shutdown().catch((error) => {
      logger.error('runtime_shutdown_failed', {
        signal,
        error: String(error),
      });
    });
    await telemetry.shutdown().catch((error) => {
      logger.warn('telemetry_shutdown_failed', {
        signal,
        error: String(error),
      });
    });

    await serverClosed;

    logger.info('shutdown_completed', {
      signal,
    });
    process.exit(0);
  }

  server.on('error', (error) => {
    logger.error('server_error', {
      error: String(error),
    });
    void shutdown('server_error');
  });

  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => {
      void shutdown(signal);
    });
  }
} catch (error) {
  logger.error('server_boot_failed', {
    error: String(error),
  });
  process.exit(1);
}
