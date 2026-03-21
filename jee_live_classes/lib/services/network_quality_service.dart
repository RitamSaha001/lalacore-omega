import 'package:flutter/material.dart';

import '../models/network_stats_model.dart';

class NetworkQualityService {
  String qualityLabel(NetworkQuality quality) {
    switch (quality) {
      case NetworkQuality.excellent:
        return 'Excellent';
      case NetworkQuality.good:
        return 'Good';
      case NetworkQuality.fair:
        return 'Fair';
      case NetworkQuality.poor:
        return 'Poor';
    }
  }

  Color qualityColor(NetworkQuality quality) {
    switch (quality) {
      case NetworkQuality.excellent:
        return const Color(0xFF00895A);
      case NetworkQuality.good:
        return const Color(0xFF1B8EDF);
      case NetworkQuality.fair:
        return const Color(0xFFD68411);
      case NetworkQuality.poor:
        return const Color(0xFFC0332E);
    }
  }

  bool shouldWarn(NetworkStatsModel stats) {
    return stats.quality == NetworkQuality.poor ||
        stats.packetLossPercent > 1.0 ||
        stats.latencyMs > 150;
  }
}
