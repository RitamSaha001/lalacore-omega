import 'dart:async';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter_tts/flutter_tts.dart';
import 'package:rive/rive.dart';

enum CompanionStatePriority { shocked, speaking, interaction, sleeping, idle }

enum _DeferredAction {
  none,
  hello,
  goodbye,
  flirty,
  thinking,
  happy,
  stretch,
  shocked,
}

class AICompanionController extends ChangeNotifier {
  SMIBool? _isTalking;
  SMIBool? _isSleeping;
  SMIBool? _showSnore;
  SMITrigger? _flirty;
  SMITrigger? _thinking;
  SMITrigger? _happy;
  SMITrigger? _stretch;
  SMITrigger? _shocked;
  SMITrigger? _hello;
  SMITrigger? _goodbye;

  final FlutterTts _tts = FlutterTts();
  final Random _random = Random();

  Timer? inactivityTimer;
  Timer? _randomTimer;
  Timer? _priorityResetTimer;

  int idleSeconds = 0;
  bool _isDisposed = false;
  bool _ttsReady = false;
  bool _lowPowerMode = false;
  bool _randomEnabled = true;
  bool _attached = false;
  _DeferredAction _deferredAction = _DeferredAction.none;

  CompanionStatePriority _currentPriority = CompanionStatePriority.idle;
  CompanionStatePriority get currentPriority => _currentPriority;

  Future<void> attachToArtboard(
    Artboard artboard, {
    String stateMachineName = 'CompanionSM',
    bool lowPowerMode = false,
    bool randomEnabled = true,
  }) async {
    final StateMachineController? machine = StateMachineController.fromArtboard(
      artboard,
      stateMachineName,
    );
    if (machine == null) {
      throw StateError(
        'State machine "$stateMachineName" was not found in companion.riv',
      );
    }

    artboard.addController(machine);
    _isTalking = machine.findSMI<SMIBool>('isTalking');
    _isSleeping = machine.findSMI<SMIBool>('isSleeping');
    _showSnore = machine.findSMI<SMIBool>('showSnore');
    _flirty = machine.findSMI<SMITrigger>('flirty');
    _thinking = machine.findSMI<SMITrigger>('thinking');
    _happy = machine.findSMI<SMITrigger>('happy');
    _stretch = machine.findSMI<SMITrigger>('stretch');
    _shocked = machine.findSMI<SMITrigger>('shocked');
    _hello = machine.findSMI<SMITrigger>('hello');
    _goodbye = machine.findSMI<SMITrigger>('goodbye');
    _attached = true;

    _lowPowerMode = lowPowerMode;
    _randomEnabled = randomEnabled;
    await _configureTts();
    _startInactivityEngine();
    _rescheduleRandomInteraction();
    _setPriority(CompanionStatePriority.idle);
    _applyDeferredAction();
  }

  Future<void> _configureTts() async {
    if (_ttsReady) {
      return;
    }
    _ttsReady = true;
    await _tts.setLanguage('en-US');
    await _tts.setSpeechRate(0.46);
    await _tts.awaitSpeakCompletion(true);
    _tts.setCompletionHandler(_onSpeechDone);
    _tts.setCancelHandler(_onSpeechDone);
    _tts.setErrorHandler((dynamic _) => _onSpeechDone());
  }

  void setLowPowerMode(bool value) {
    _lowPowerMode = value;
    _rescheduleRandomInteraction();
  }

  void setRandomEnabled(bool value) {
    _randomEnabled = value;
    _rescheduleRandomInteraction();
  }

  Future<void> playHello() async {
    if (!_attached) {
      _deferredAction = _DeferredAction.hello;
      return;
    }
    _wakeFromSleep();
    if (_fireTrigger(
      _hello,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 780),
    )) {
      return;
    }
    _fireTrigger(
      _happy,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 720),
    );
  }

  Future<void> playGoodbye() async {
    if (!_attached) {
      _deferredAction = _DeferredAction.goodbye;
      return;
    }
    if (_fireTrigger(
      _goodbye,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 620),
    )) {
      await Future<void>.delayed(const Duration(milliseconds: 420));
      return;
    }
    _fireTrigger(
      _thinking,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 560),
    );
    await Future<void>.delayed(const Duration(milliseconds: 360));
  }

  void playFlirty() {
    if (!_attached) {
      _deferredAction = _DeferredAction.flirty;
      return;
    }
    _fireTrigger(
      _flirty,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 760),
    );
  }

  void playThinking() {
    if (!_attached) {
      _deferredAction = _DeferredAction.thinking;
      return;
    }
    _fireTrigger(
      _thinking,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 820),
    );
  }

  void playHappy() {
    if (!_attached) {
      _deferredAction = _DeferredAction.happy;
      return;
    }
    _fireTrigger(
      _happy,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 720),
    );
  }

  void playStretch() {
    if (!_attached) {
      _deferredAction = _DeferredAction.stretch;
      return;
    }
    _fireTrigger(
      _stretch,
      priority: CompanionStatePriority.interaction,
      hold: const Duration(milliseconds: 860),
    );
  }

  void playShocked() {
    if (!_attached) {
      _deferredAction = _DeferredAction.shocked;
      return;
    }
    _wakeFromSleep();
    _fireTrigger(
      _shocked,
      priority: CompanionStatePriority.shocked,
      hold: const Duration(milliseconds: 480),
    );
  }

  void setSleeping(bool value) {
    if (!_attached) {
      return;
    }
    if (value) {
      if (!_canRun(CompanionStatePriority.sleeping)) {
        return;
      }
      _isTalking?.value = false;
      _showSnore?.value = false;
      _isSleeping?.value = true;
      _setPriority(CompanionStatePriority.sleeping);
      return;
    }
    _wakeFromSleep();
  }

  void registerUserInteraction() {
    if (!_attached) {
      return;
    }
    idleSeconds = 0;
    _wakeFromSleep();
    if (_canRun(CompanionStatePriority.shocked)) {
      playShocked();
    }
  }

  Future<void> speak(String text) async {
    if (!_attached) {
      return;
    }
    final String trimmed = text.trim();
    if (trimmed.isEmpty) {
      return;
    }
    if (!_canRun(CompanionStatePriority.speaking) &&
        _currentPriority != CompanionStatePriority.speaking) {
      return;
    }
    _wakeFromSleep();
    _showSnore?.value = false;
    await _configureTts();
    await _tts.stop();
    _isTalking?.value = true;
    _setPriority(CompanionStatePriority.speaking);
    idleSeconds = 0;
    final String speakText = trimmed.length > 340
        ? '${trimmed.substring(0, 340)}...'
        : trimmed;
    await _tts.speak(speakText);
  }

  Future<void> reactToAIResponse({
    required String text,
    required double confidenceScore,
    required bool isCorrectAnswer,
  }) async {
    if (!_attached) {
      return;
    }
    await speak(text);
    if (_currentPriority.index <= CompanionStatePriority.speaking.index) {
      return;
    }
    if (confidenceScore > 0.85) {
      playHappy();
    } else if (confidenceScore < 0.5) {
      playThinking();
    }
    if (!isCorrectAnswer) {
      playFlirty();
    }
  }

  void _startInactivityEngine() {
    if (!_attached) {
      return;
    }
    inactivityTimer?.cancel();
    inactivityTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      if (_isDisposed) {
        return;
      }
      if (_currentPriority == CompanionStatePriority.speaking) {
        return;
      }
      idleSeconds += 1;
      final int stretchAt = _lowPowerMode ? 26 : 20;
      final int sleepAt = _lowPowerMode ? 52 : 40;
      final int snoreAt = _lowPowerMode ? 80 : 60;
      if (idleSeconds == stretchAt) {
        playStretch();
      } else if (idleSeconds == sleepAt) {
        setSleeping(true);
      } else if (idleSeconds == snoreAt) {
        if ((_isSleeping?.value ?? false) && !(_isTalking?.value ?? false)) {
          _showSnore?.value = true;
        }
      }
    });
  }

  void _rescheduleRandomInteraction() {
    _randomTimer?.cancel();
    if (_lowPowerMode || !_randomEnabled || !_attached) {
      return;
    }
    final int waitSeconds = 120 + _random.nextInt(121);
    _randomTimer = Timer(Duration(seconds: waitSeconds), () {
      if (_isDisposed) {
        return;
      }
      final bool canInteract =
          _currentPriority == CompanionStatePriority.idle &&
          !(_isSleeping?.value ?? false) &&
          !(_isTalking?.value ?? false);
      if (canInteract) {
        switch (_random.nextInt(3)) {
          case 0:
            playFlirty();
            break;
          case 1:
            playStretch();
            break;
          default:
            playHappy();
            break;
        }
      }
      _rescheduleRandomInteraction();
    });
  }

  bool _fireTrigger(
    SMITrigger? trigger, {
    required CompanionStatePriority priority,
    required Duration hold,
  }) {
    if (trigger == null || !_canRun(priority)) {
      return false;
    }
    if (priority.index <= CompanionStatePriority.interaction.index) {
      _wakeFromSleep();
    }
    _showSnore?.value = false;
    trigger.fire();
    _setPriority(priority, hold: hold);
    return true;
  }

  bool _canRun(CompanionStatePriority incoming) {
    return _rank(incoming) >= _rank(_currentPriority);
  }

  int _rank(CompanionStatePriority p) {
    return switch (p) {
      CompanionStatePriority.idle => 0,
      CompanionStatePriority.sleeping => 1,
      CompanionStatePriority.interaction => 2,
      CompanionStatePriority.speaking => 3,
      CompanionStatePriority.shocked => 4,
    };
  }

  void _setPriority(CompanionStatePriority next, {Duration? hold}) {
    _currentPriority = next;
    _priorityResetTimer?.cancel();
    if (hold != null) {
      _priorityResetTimer = Timer(hold, _softResetPriority);
    }
    notifyListeners();
  }

  void _softResetPriority() {
    if (_isDisposed) {
      return;
    }
    if (_isTalking?.value ?? false) {
      _currentPriority = CompanionStatePriority.speaking;
    } else if (_isSleeping?.value ?? false) {
      _currentPriority = CompanionStatePriority.sleeping;
    } else {
      _currentPriority = CompanionStatePriority.idle;
    }
    notifyListeners();
  }

  void _wakeFromSleep() {
    _isSleeping?.value = false;
    _showSnore?.value = false;
    if (_currentPriority == CompanionStatePriority.sleeping) {
      _setPriority(CompanionStatePriority.idle);
    }
  }

  void _onSpeechDone() {
    _isTalking?.value = false;
    _softResetPriority();
  }

  void _applyDeferredAction() {
    final _DeferredAction deferred = _deferredAction;
    _deferredAction = _DeferredAction.none;
    switch (deferred) {
      case _DeferredAction.none:
        return;
      case _DeferredAction.hello:
        unawaited(playHello());
        return;
      case _DeferredAction.goodbye:
        unawaited(playGoodbye());
        return;
      case _DeferredAction.flirty:
        playFlirty();
        return;
      case _DeferredAction.thinking:
        playThinking();
        return;
      case _DeferredAction.happy:
        playHappy();
        return;
      case _DeferredAction.stretch:
        playStretch();
        return;
      case _DeferredAction.shocked:
        playShocked();
        return;
    }
  }

  @override
  void dispose() {
    _isDisposed = true;
    _attached = false;
    inactivityTimer?.cancel();
    _randomTimer?.cancel();
    _priorityResetTimer?.cancel();
    unawaited(_tts.stop());
    super.dispose();
  }
}
