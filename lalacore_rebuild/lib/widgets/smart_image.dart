import 'dart:convert';

import 'package:cached_network_image/cached_network_image.dart';
import 'package:flutter/material.dart';

class SmartImage extends StatelessWidget {
  const SmartImage(
    this.source, {
    super.key,
    this.width,
    this.height,
    this.fit = BoxFit.cover,
  });

  final String source;
  final double? width;
  final double? height;
  final BoxFit fit;

  @override
  Widget build(BuildContext context) {
    if (source.isEmpty) {
      return const SizedBox.shrink();
    }

    if (source.startsWith('data:image')) {
      try {
        final String base64String = source.split(',').last;
        return Image.memory(
          base64Decode(base64String),
          width: width,
          height: height,
          fit: fit,
          errorBuilder: (_, __, ___) => const Icon(Icons.broken_image),
        );
      } catch (_) {
        return const Icon(Icons.error, color: Colors.red);
      }
    }

    if (source.startsWith('http')) {
      return CachedNetworkImage(
        imageUrl: source,
        width: width,
        height: height,
        fit: fit,
        errorWidget: (_, __, ___) => const Icon(Icons.broken_image),
        placeholder: (_, __) => Container(
          color: Colors.grey.withOpacity(0.1),
          child: const Center(child: CircularProgressIndicator(strokeWidth: 2)),
        ),
      );
    }

    return const SizedBox.shrink();
  }
}
