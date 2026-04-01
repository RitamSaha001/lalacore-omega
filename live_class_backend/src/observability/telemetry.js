export async function startTelemetry({ config, logger }) {
  if (!config.observability.otelEnabled) {
    logger.info('otel_disabled', {
      reason: 'flag_disabled',
    });
    return {
      enabled: false,
      shutdown: async () => {},
    };
  }

  try {
    const [
      { NodeSDK },
      { getNodeAutoInstrumentations },
      { OTLPTraceExporter },
      { resourceFromAttributes },
      { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION },
    ] = await Promise.all([
      import('@opentelemetry/sdk-node'),
      import('@opentelemetry/auto-instrumentations-node'),
      import('@opentelemetry/exporter-trace-otlp-http'),
      import('@opentelemetry/resources'),
      import('@opentelemetry/semantic-conventions'),
    ]);

    const traceExporter = config.observability.otelExporterUrl
      ? new OTLPTraceExporter({
          url: config.observability.otelExporterUrl,
        })
      : undefined;

    const sdk = new NodeSDK({
      resource: resourceFromAttributes({
        [ATTR_SERVICE_NAME]: config.observability.otelServiceName,
        [ATTR_SERVICE_VERSION]: config.observability.otelServiceVersion,
        'service.instance.id': config.nodeId,
      }),
      traceExporter,
      instrumentations: [getNodeAutoInstrumentations()],
    });

    await sdk.start();
    logger.info('otel_started', {
      exporterUrl: config.observability.otelExporterUrl || null,
      serviceName: config.observability.otelServiceName,
      serviceVersion: config.observability.otelServiceVersion,
    });

    return {
      enabled: true,
      shutdown: async () => {
        await sdk.shutdown().catch((error) => {
          logger.warn('otel_shutdown_failed', {
            error: String(error),
          });
        });
      },
    };
  } catch (error) {
    logger.error('otel_start_failed', {
      error: String(error),
    });
    return {
      enabled: false,
      shutdown: async () => {},
    };
  }
}
