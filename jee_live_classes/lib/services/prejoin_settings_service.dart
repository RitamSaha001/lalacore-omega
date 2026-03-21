import 'package:hive_flutter/hive_flutter.dart';

class PreJoinSettings {
  const PreJoinSettings({
    required this.cameraEnabled,
    required this.micEnabled,
    required this.speakerTested,
  });

  final bool cameraEnabled;
  final bool micEnabled;
  final bool speakerTested;

  PreJoinSettings copyWith({
    bool? cameraEnabled,
    bool? micEnabled,
    bool? speakerTested,
  }) {
    return PreJoinSettings(
      cameraEnabled: cameraEnabled ?? this.cameraEnabled,
      micEnabled: micEnabled ?? this.micEnabled,
      speakerTested: speakerTested ?? this.speakerTested,
    );
  }

  static const defaults = PreJoinSettings(
    cameraEnabled: true,
    micEnabled: true,
    speakerTested: false,
  );
}

class PreJoinSettingsService {
  static const _boxName = 'prejoin_settings_box';
  bool _initialized = false;

  Future<void> _ensureInitialized() async {
    if (_initialized) {
      return;
    }
    await Hive.initFlutter();
    await Hive.openBox<Map>(_boxName);
    _initialized = true;
  }

  String _key({required String classId, required String userId}) {
    return '$classId::$userId';
  }

  Future<PreJoinSettings> load({
    required String classId,
    required String userId,
  }) async {
    await _ensureInitialized();
    final box = Hive.box<Map>(_boxName);
    final raw = box.get(_key(classId: classId, userId: userId));
    if (raw == null) {
      return PreJoinSettings.defaults;
    }
    return PreJoinSettings(
      cameraEnabled: raw['camera_enabled'] != false,
      micEnabled: raw['mic_enabled'] != false,
      speakerTested: raw['speaker_tested'] == true,
    );
  }

  Future<void> save({
    required String classId,
    required String userId,
    required PreJoinSettings settings,
  }) async {
    await _ensureInitialized();
    final box = Hive.box<Map>(_boxName);
    await box.put(_key(classId: classId, userId: userId), {
      'camera_enabled': settings.cameraEnabled,
      'mic_enabled': settings.micEnabled,
      'speaker_tested': settings.speakerTested,
    });
  }
}
