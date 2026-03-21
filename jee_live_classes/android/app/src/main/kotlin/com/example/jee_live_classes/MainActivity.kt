package com.example.jee_live_classes

import android.os.Handler
import android.os.Looper
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.Timer
import kotlin.concurrent.fixedRateTimer
import kotlin.random.Random

class MainActivity : FlutterActivity() {
    private val channelName = "jee_live_classes/zoom_videosdk"
    private lateinit var methodChannel: MethodChannel

    private val mainHandler = Handler(Looper.getMainLooper())

    private val participants = linkedMapOf<String, MutableMap<String, Any>>()
    private val waitingRoom = linkedMapOf<String, MutableMap<String, Any>>()

    private var meetingLocked = false
    private var chatEnabled = true
    private var currentSessionId: String? = null
    private var currentToken: String? = null
    private var localParticipantId: String = "student_01"

    private var networkTimer: Timer? = null
    private var reactionTimer: Timer? = null

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        methodChannel = MethodChannel(flutterEngine.dartExecutor.binaryMessenger, channelName)
        methodChannel.setMethodCallHandler { call, result ->
            handleMethodCall(call, result)
        }
    }

    override fun onDestroy() {
        stopTimers()
        super.onDestroy()
    }

    private fun handleMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "initializeZoom" -> {
                initializeState()
                result.success(null)
            }
            "bridgeStatus" -> {
                result.success(
                    mapOf(
                        "implementation" to "simulated_native_bridge",
                        "channel" to channelName,
                    )
                )
            }
            "joinSession" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val sessionId = args["sessionId"]?.toString().orEmpty()
                val token = args["token"]?.toString().orEmpty()
                val displayName = args["displayName"]?.toString().orEmpty()
                if (meetingLocked) {
                    result.error("MEETING_LOCKED", "Meeting is locked by host", null)
                    return
                }
                joinSession(sessionId = sessionId, token = token, displayName = displayName)
                result.success(null)
            }
            "leaveSession" -> {
                leaveSession()
                result.success(null)
            }
            "toggleMic" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                val enabled = args["enabled"] as? Boolean ?: true
                updateParticipant(participantId) { item -> item["micEnabled"] = enabled }
                emit("onUserAudioStatusChanged", participantList())
                result.success(null)
            }
            "toggleCamera" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                val enabled = args["enabled"] as? Boolean ?: true
                updateParticipant(participantId) { item -> item["cameraEnabled"] = enabled }
                emit("onUserVideoStatusChanged", participantList())
                result.success(null)
            }
            "setRaiseHand" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                val raised = args["raised"] as? Boolean ?: false
                updateParticipant(participantId) { item -> item["handRaised"] = raised }
                emit("onParticipantsUpdated", participantList())
                result.success(null)
            }
            "muteParticipant" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                updateParticipant(participantId) { item -> item["micEnabled"] = false }
                emit("onUserAudioStatusChanged", participantList())
                result.success(null)
            }
            "removeParticipant" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                participants.remove(participantId)
                emit("onUserLeave", participantList())
                result.success(null)
            }
            "disableParticipantCamera" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                updateParticipant(participantId) { item -> item["cameraEnabled"] = false }
                emit("onUserVideoStatusChanged", participantList())
                result.success(null)
            }
            "promoteCoHost" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                updateParticipant(participantId) { item -> item["role"] = "cohost" }
                emit("onParticipantsUpdated", participantList())
                result.success(null)
            }
            "muteAll" -> {
                participants.values.forEach { participant ->
                    val role = participant["role"]?.toString().orEmpty()
                    if (role != "host") {
                        participant["micEnabled"] = false
                    }
                }
                emit("onUserAudioStatusChanged", participantList())
                result.success(null)
            }
            "setChatEnabled" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                chatEnabled = args["enabled"] as? Boolean ?: true
                result.success(null)
            }
            "lockMeeting" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                meetingLocked = args["locked"] as? Boolean ?: false
                emit("onMeetingLocked", mapOf("locked" to meetingLocked))
                result.success(null)
            }
            "pinParticipant" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                emit("onActiveSpeakerChanged", mapOf("participantId" to participantId))
                result.success(null)
            }
            "unpinParticipant" -> {
                emit("onActiveSpeakerChanged", mapOf("participantId" to ""))
                result.success(null)
            }
            "startScreenShare" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val source = args["source"]?.toString().orEmpty()
                participants.values.firstOrNull { it["role"] == "host" }?.set("isScreenSharing", true)
                emit("onScreenShareStatusChanged", mapOf("source" to source, "active" to true))
                emit("onParticipantsUpdated", participantList())
                result.success(null)
            }
            "stopScreenShare" -> {
                participants.values.firstOrNull { it["role"] == "host" }?.set("isScreenSharing", false)
                emit("onScreenShareStatusChanged", mapOf("source" to "", "active" to false))
                emit("onParticipantsUpdated", participantList())
                result.success(null)
            }
            "startRecording" -> {
                result.success(null)
            }
            "stopRecording" -> {
                result.success(null)
            }
            "sendReaction" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val emoji = args["emoji"]?.toString().orEmpty()
                emit("onReaction", mapOf("emoji" to emoji))
                result.success(null)
            }
            "subscribeVideoStream",
            "unsubscribeVideoStream",
            "joinBreakoutRoom",
            "leaveBreakoutRoom",
            "broadcastMessageToRooms" -> {
                result.success(null)
            }
            "approveWaitingUser" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                approveWaitingUser(participantId)
                result.success(null)
            }
            "rejectWaitingUser" -> {
                val args = (call.arguments as? Map<*, *>) ?: emptyMap<String, Any>()
                val participantId = args["participantId"]?.toString().orEmpty()
                waitingRoom.remove(participantId)
                emitWaitingRoomSnapshot()
                result.success(null)
            }
            else -> result.notImplemented()
        }
    }

    private fun initializeState() {
        participants.clear()
        waitingRoom.clear()
        meetingLocked = false
        chatEnabled = true

        participants["teacher_01"] = mutableMapOf(
            "id" to "teacher_01",
            "name" to "Dr. A. Sharma",
            "role" to "host",
            "micEnabled" to true,
            "cameraEnabled" to true,
            "handRaised" to false,
            "isScreenSharing" to false,
            "networkQuality" to 3,
        )

        waitingRoom["student_wait_01"] = mutableMapOf(
            "participantId" to "student_wait_01",
            "name" to "Rahul",
            "requestedAt" to nowIso(),
        )

        emit("onMeetingLocked", mapOf("locked" to meetingLocked))
        emitWaitingRoomSnapshot()
    }

    private fun joinSession(sessionId: String, token: String, displayName: String) {
        currentSessionId = sessionId
        currentToken = token
        localParticipantId = if (displayName.isBlank()) "student_01" else displayName

        val exists = participants.containsKey(localParticipantId)
        if (!exists) {
            participants[localParticipantId] = mutableMapOf(
                "id" to localParticipantId,
                "name" to localParticipantId,
                "role" to "student",
                "micEnabled" to true,
                "cameraEnabled" to true,
                "handRaised" to false,
                "isScreenSharing" to false,
                "networkQuality" to 2,
            )
        }

        emit("onParticipantsUpdated", participantList())
        emit("onActiveSpeakerChanged", mapOf("participantId" to "teacher_01"))
        emit("onReconnected", null)

        startTimers()
    }

    private fun leaveSession() {
        stopTimers()
        currentSessionId = null
        currentToken = null
        emit("onConnectionFailed", null)
    }

    private fun approveWaitingUser(participantId: String) {
        val pending = waitingRoom.remove(participantId) ?: return
        participants[participantId] = mutableMapOf(
            "id" to participantId,
            "name" to (pending["name"]?.toString() ?: "Student"),
            "role" to "student",
            "micEnabled" to false,
            "cameraEnabled" to true,
            "handRaised" to false,
            "isScreenSharing" to false,
            "networkQuality" to 2,
        )
        emitWaitingRoomSnapshot()
        emit("onUserJoin", participantList())
    }

    private fun updateParticipant(
        participantId: String,
        updater: (MutableMap<String, Any>) -> Unit,
    ) {
        val participant = participants[participantId] ?: return
        updater(participant)
    }

    private fun participantList(): List<Map<String, Any>> {
        return participants.values.map { HashMap(it) }
    }

    private fun waitingList(): List<Map<String, Any>> {
        return waitingRoom.values.map { HashMap(it) }
    }

    private fun emitWaitingRoomSnapshot() {
        emit("onWaitingRoomUpdated", waitingList())
    }

    private fun emit(method: String, args: Any?) {
        mainHandler.post {
            if (::methodChannel.isInitialized) {
                methodChannel.invokeMethod(method, args)
            }
        }
    }

    private fun startTimers() {
        stopTimers()

        networkTimer = fixedRateTimer(name = "zoom-network-stats", daemon = true, initialDelay = 1200L, period = 2500L) {
            val latency = Random.nextInt(28, 96)
            val packetLoss = Random.nextDouble(0.0, 0.8)
            val jitter = Random.nextInt(4, 26)
            val uplink = Random.nextInt(1100, 2800)
            val downlink = Random.nextInt(1300, 3200)
            val quality = when {
                latency < 45 && packetLoss < 0.2 -> 3
                latency < 70 && packetLoss < 0.4 -> 2
                latency < 95 && packetLoss < 0.7 -> 1
                else -> 0
            }
            emit(
                "onNetworkQualityChanged",
                mapOf(
                    "latencyMs" to latency,
                    "packetLoss" to packetLoss,
                    "jitterMs" to jitter,
                    "uplinkKbps" to uplink,
                    "downlinkKbps" to downlink,
                    "quality" to quality,
                ),
            )
        }

        reactionTimer = fixedRateTimer(name = "zoom-reactions", daemon = true, initialDelay = 5000L, period = 9000L) {
            if (!chatEnabled) {
                return@fixedRateTimer
            }
            val choices = listOf("👍", "👏", "🔥", "❓")
            emit("onReaction", mapOf("emoji" to choices.random()))
        }
    }

    private fun stopTimers() {
        networkTimer?.cancel()
        networkTimer = null
        reactionTimer?.cancel()
        reactionTimer = null
    }

    private fun nowIso(): String {
        val format = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
        format.timeZone = TimeZone.getTimeZone("UTC")
        return format.format(Date())
    }
}
