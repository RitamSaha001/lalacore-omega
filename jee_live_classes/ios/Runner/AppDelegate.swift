import Flutter
import UIKit

@main
@objc class AppDelegate: FlutterAppDelegate {
  private let channelName = "jee_live_classes/zoom_videosdk"
  private var methodChannel: FlutterMethodChannel?

  private var participants: [String: [String: Any]] = [:]
  private var waitingRoom: [String: [String: Any]] = [:]

  private var meetingLocked = false
  private var chatEnabled = true
  private var currentSessionId: String?
  private var currentToken: String?
  private var localParticipantId = "student_01"

  private var networkTimer: Timer?
  private var reactionTimer: Timer?

  override func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
  ) -> Bool {
    GeneratedPluginRegistrant.register(with: self)
    let didFinish = super.application(application, didFinishLaunchingWithOptions: launchOptions)
    attachMethodChannelIfPossible()
    DispatchQueue.main.async { [weak self] in
      self?.attachMethodChannelIfPossible()
    }
    return didFinish
  }

  deinit {
    stopTimers()
  }

  private func attachMethodChannelIfPossible() {
    if methodChannel != nil {
      return
    }
    guard let controller = window?.rootViewController as? FlutterViewController else {
      return
    }
    let channel = FlutterMethodChannel(name: channelName, binaryMessenger: controller.binaryMessenger)
    methodChannel = channel
    channel.setMethodCallHandler { [weak self] call, result in
      self?.handle(call: call, result: result)
    }
  }

  private func handle(call: FlutterMethodCall, result: @escaping FlutterResult) {
    let args = call.arguments as? [String: Any] ?? [:]

    switch call.method {
    case "bridgeStatus":
      result([
        "implementation": "simulated_native_bridge",
        "channel": channelName,
      ])

    case "initializeZoom":
      initializeState()
      result(nil)

    case "joinSession":
      let sessionId = (args["sessionId"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
      let token = (args["token"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
      let displayName = (args["displayName"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
      if meetingLocked {
        result(
          FlutterError(
            code: "MEETING_LOCKED",
            message: "Meeting is locked by host",
            details: nil
          )
        )
        return
      }
      joinSession(sessionId: sessionId, token: token, displayName: displayName)
      result(nil)

    case "leaveSession":
      leaveSession()
      result(nil)

    case "toggleMic":
      let participantId = args["participantId"] as? String ?? ""
      let enabled = args["enabled"] as? Bool ?? true
      updateParticipant(participantId) { item in item["micEnabled"] = enabled }
      emit("onUserAudioStatusChanged", args: participantList())
      result(nil)

    case "toggleCamera":
      let participantId = args["participantId"] as? String ?? ""
      let enabled = args["enabled"] as? Bool ?? true
      updateParticipant(participantId) { item in item["cameraEnabled"] = enabled }
      emit("onUserVideoStatusChanged", args: participantList())
      result(nil)

    case "setRaiseHand":
      let participantId = args["participantId"] as? String ?? ""
      let raised = args["raised"] as? Bool ?? false
      updateParticipant(participantId) { item in item["handRaised"] = raised }
      emit("onParticipantsUpdated", args: participantList())
      result(nil)

    case "muteParticipant":
      let participantId = args["participantId"] as? String ?? ""
      updateParticipant(participantId) { item in item["micEnabled"] = false }
      emit("onUserAudioStatusChanged", args: participantList())
      result(nil)

    case "removeParticipant":
      let participantId = args["participantId"] as? String ?? ""
      participants.removeValue(forKey: participantId)
      emit("onUserLeave", args: participantList())
      result(nil)

    case "disableParticipantCamera":
      let participantId = args["participantId"] as? String ?? ""
      updateParticipant(participantId) { item in item["cameraEnabled"] = false }
      emit("onUserVideoStatusChanged", args: participantList())
      result(nil)

    case "promoteCoHost":
      let participantId = args["participantId"] as? String ?? ""
      updateParticipant(participantId) { item in item["role"] = "cohost" }
      emit("onParticipantsUpdated", args: participantList())
      result(nil)

    case "muteAll":
      participants.keys.forEach { key in
        guard var participant = participants[key] else { return }
        let role = participant["role"] as? String ?? ""
        if role != "host" {
          participant["micEnabled"] = false
          participants[key] = participant
        }
      }
      emit("onUserAudioStatusChanged", args: participantList())
      result(nil)

    case "setChatEnabled":
      chatEnabled = args["enabled"] as? Bool ?? true
      result(nil)

    case "lockMeeting":
      meetingLocked = args["locked"] as? Bool ?? false
      emit("onMeetingLocked", args: ["locked": meetingLocked])
      result(nil)

    case "pinParticipant":
      let participantId = args["participantId"] as? String ?? ""
      emit("onActiveSpeakerChanged", args: ["participantId": participantId])
      result(nil)

    case "unpinParticipant":
      emit("onActiveSpeakerChanged", args: ["participantId": ""])
      result(nil)

    case "startScreenShare":
      let source = args["source"] as? String ?? ""
      if let hostId = participants.first(where: { ($0.value["role"] as? String) == "host" })?.key,
         var host = participants[hostId] {
        host["isScreenSharing"] = true
        participants[hostId] = host
      }
      emit("onScreenShareStatusChanged", args: ["source": source, "active": true])
      emit("onParticipantsUpdated", args: participantList())
      result(nil)

    case "stopScreenShare":
      if let hostId = participants.first(where: { ($0.value["role"] as? String) == "host" })?.key,
         var host = participants[hostId] {
        host["isScreenSharing"] = false
        participants[hostId] = host
      }
      emit("onScreenShareStatusChanged", args: ["source": "", "active": false])
      emit("onParticipantsUpdated", args: participantList())
      result(nil)

    case "startRecording", "stopRecording", "subscribeVideoStream", "unsubscribeVideoStream", "joinBreakoutRoom", "leaveBreakoutRoom", "broadcastMessageToRooms":
      result(nil)

    case "sendReaction":
      let emoji = args["emoji"] as? String ?? "👍"
      emit("onReaction", args: ["emoji": emoji])
      result(nil)

    case "approveWaitingUser":
      let participantId = args["participantId"] as? String ?? ""
      approveWaitingUser(participantId)
      result(nil)

    case "rejectWaitingUser":
      let participantId = args["participantId"] as? String ?? ""
      waitingRoom.removeValue(forKey: participantId)
      emitWaitingRoomSnapshot()
      result(nil)

    default:
      result(FlutterMethodNotImplemented)
    }
  }

  private func initializeState() {
    participants.removeAll()
    waitingRoom.removeAll()
    meetingLocked = false
    chatEnabled = true

    participants["teacher_01"] = [
      "id": "teacher_01",
      "name": "Dr. A. Sharma",
      "role": "host",
      "micEnabled": true,
      "cameraEnabled": true,
      "handRaised": false,
      "isScreenSharing": false,
      "networkQuality": 3,
    ]

    waitingRoom["student_wait_01"] = [
      "participantId": "student_wait_01",
      "name": "Rahul",
      "requestedAt": nowIso(),
    ]

    emit("onMeetingLocked", args: ["locked": meetingLocked])
    emitWaitingRoomSnapshot()
  }

  private func joinSession(sessionId: String, token: String, displayName: String) {
    currentSessionId = sessionId
    currentToken = token
    localParticipantId = displayName.isEmpty ? "student_01" : displayName

    if participants[localParticipantId] == nil {
      participants[localParticipantId] = [
        "id": localParticipantId,
        "name": localParticipantId,
        "role": "student",
        "micEnabled": true,
        "cameraEnabled": true,
        "handRaised": false,
        "isScreenSharing": false,
        "networkQuality": 2,
      ]
    }

    emit("onParticipantsUpdated", args: participantList())
    emit("onActiveSpeakerChanged", args: ["participantId": "teacher_01"])
    emit("onReconnected", args: nil)

    startTimers()
  }

  private func leaveSession() {
    stopTimers()
    currentSessionId = nil
    currentToken = nil
    emit("onConnectionFailed", args: nil)
  }

  private func approveWaitingUser(_ participantId: String) {
    guard let pending = waitingRoom.removeValue(forKey: participantId) else {
      return
    }
    participants[participantId] = [
      "id": participantId,
      "name": pending["name"] as? String ?? "Student",
      "role": "student",
      "micEnabled": false,
      "cameraEnabled": true,
      "handRaised": false,
      "isScreenSharing": false,
      "networkQuality": 2,
    ]
    emitWaitingRoomSnapshot()
    emit("onUserJoin", args: participantList())
  }

  private func updateParticipant(_ participantId: String, update: (inout [String: Any]) -> Void) {
    guard var participant = participants[participantId] else {
      return
    }
    update(&participant)
    participants[participantId] = participant
  }

  private func participantList() -> [[String: Any]] {
    return participants.values.map { $0 }
  }

  private func waitingList() -> [[String: Any]] {
    return waitingRoom.values.map { $0 }
  }

  private func emitWaitingRoomSnapshot() {
    emit("onWaitingRoomUpdated", args: waitingList())
  }

  private func emit(_ method: String, args: Any?) {
    DispatchQueue.main.async { [weak self] in
      self?.methodChannel?.invokeMethod(method, arguments: args)
    }
  }

  private func startTimers() {
    stopTimers()

    networkTimer = Timer.scheduledTimer(withTimeInterval: 2.5, repeats: true) { [weak self] _ in
      guard let self = self else { return }
      let latency = Int.random(in: 28...95)
      let packetLoss = Double.random(in: 0...0.8)
      let jitter = Int.random(in: 4...25)
      let uplink = Int.random(in: 1100...2800)
      let downlink = Int.random(in: 1300...3200)
      let quality: Int
      if latency < 45 && packetLoss < 0.2 {
        quality = 3
      } else if latency < 70 && packetLoss < 0.4 {
        quality = 2
      } else if latency < 95 && packetLoss < 0.7 {
        quality = 1
      } else {
        quality = 0
      }

      self.emit(
        "onNetworkQualityChanged",
        args: [
          "latencyMs": latency,
          "packetLoss": packetLoss,
          "jitterMs": jitter,
          "uplinkKbps": uplink,
          "downlinkKbps": downlink,
          "quality": quality,
        ]
      )
    }

    reactionTimer = Timer.scheduledTimer(withTimeInterval: 9.0, repeats: true) { [weak self] _ in
      guard let self = self else { return }
      guard self.chatEnabled else { return }
      let reactions = ["👍", "👏", "🔥", "❓"]
      let selected = reactions.randomElement() ?? "👍"
      self.emit("onReaction", args: ["emoji": selected])
    }
  }

  private func stopTimers() {
    networkTimer?.invalidate()
    reactionTimer?.invalidate()
    networkTimer = nil
    reactionTimer = nil
  }

  private func nowIso() -> String {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime]
    return formatter.string(from: Date())
  }
}
