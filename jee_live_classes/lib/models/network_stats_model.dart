enum NetworkQuality { poor, fair, good, excellent }

class NetworkStatsModel {
  const NetworkStatsModel({
    required this.latencyMs,
    required this.packetLossPercent,
    required this.jitterMs,
    required this.uplinkKbps,
    required this.downlinkKbps,
    required this.quality,
  });

  final int latencyMs;
  final double packetLossPercent;
  final int jitterMs;
  final int uplinkKbps;
  final int downlinkKbps;
  final NetworkQuality quality;

  NetworkStatsModel copyWith({
    int? latencyMs,
    double? packetLossPercent,
    int? jitterMs,
    int? uplinkKbps,
    int? downlinkKbps,
    NetworkQuality? quality,
  }) {
    return NetworkStatsModel(
      latencyMs: latencyMs ?? this.latencyMs,
      packetLossPercent: packetLossPercent ?? this.packetLossPercent,
      jitterMs: jitterMs ?? this.jitterMs,
      uplinkKbps: uplinkKbps ?? this.uplinkKbps,
      downlinkKbps: downlinkKbps ?? this.downlinkKbps,
      quality: quality ?? this.quality,
    );
  }
}
