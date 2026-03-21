import 'package:flutter/material.dart';

import '../models/network_stats_model.dart';
import '../services/network_quality_service.dart';

class NetworkIndicator extends StatelessWidget {
  const NetworkIndicator({
    super.key,
    required this.stats,
    required this.qualityService,
  });

  final NetworkStatsModel stats;
  final NetworkQualityService qualityService;

  @override
  Widget build(BuildContext context) {
    final qualityLabel = qualityService.qualityLabel(stats.quality);
    final qualityColor = qualityService.qualityColor(stats.quality);

    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: const Color(0xFFD8E4F5)),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.network_check, color: qualityColor, size: 17),
            const SizedBox(width: 6),
            Text(
              '$qualityLabel (${stats.latencyMs} ms)',
              style: const TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
            ),
          ],
        ),
      ),
    );
  }
}
