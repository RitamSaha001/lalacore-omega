import { createBreakoutRoom } from '../../models/breakoutRoom.js';
import { createChatMessage } from '../../models/chatMessage.js';
import { createClassSession } from '../../models/classSession.js';
import { createParticipant } from '../../models/participant.js';
import { createRecordingJob } from '../../models/recordingJob.js';
import { createRefreshToken } from '../../models/refreshToken.js';
import { createUser } from '../../models/user.js';

export function mapClassSessionRow(row) {
  if (!row) {
    return null;
  }
  return createClassSession({
    id: row.id,
    title: row.title,
    teacherId: row.teacher_id,
    teacherName: row.teacher_name,
    activeRoomId: row.active_room_id,
    chatEnabled: row.chat_enabled,
    meetingLocked: row.meeting_locked,
    waitingRoomEnabled: row.waiting_room_enabled,
    isRecording: row.is_recording,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
    activeWhiteboardUserId: row.active_whiteboard_user_id,
    whiteboardStrokes: row.whiteboard_strokes ?? [],
    activeRecording: row.active_recording,
    recordingStatus: row.recording_status,
    version: row.version,
  });
}

export function mapParticipantRow(row) {
  if (!row) {
    return null;
  }
  return createParticipant({
    id: row.id,
    userId: row.user_id,
    classId: row.class_id,
    userName: row.user_name,
    role: row.role,
    status: row.status,
    breakoutRoomId: row.breakout_room_id,
    muted: row.muted,
    cameraDisabled: row.camera_disabled,
    whiteboardAccess: row.whiteboard_access,
    requestId: row.request_id,
    requestedAt: row.requested_at?.toISOString?.() ?? row.requested_at,
    approvedAt: row.approved_at?.toISOString?.() ?? row.approved_at,
    rejectedAt: row.rejected_at?.toISOString?.() ?? row.rejected_at,
    presenceStatus: row.presence_status,
    lastSeenAt: row.last_seen_at?.toISOString?.() ?? row.last_seen_at,
    disconnectedAt:
      row.disconnected_at?.toISOString?.() ?? row.disconnected_at,
    disconnectGraceExpiresAt:
      row.disconnect_grace_expires_at?.toISOString?.() ??
      row.disconnect_grace_expires_at,
    version: row.version,
  });
}

export function mapBreakoutRoomRow(row) {
  if (!row) {
    return null;
  }
  return createBreakoutRoom({
    id: row.id,
    classId: row.class_id,
    name: row.name,
    livekitRoomName: row.livekit_room_name,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
    version: row.version,
  });
}

export function mapChatMessageRow(row) {
  if (!row) {
    return null;
  }
  return createChatMessage({
    id: row.id,
    classId: row.class_id,
    senderId: row.sender_id,
    senderName: row.sender_name,
    message: row.message,
    attachment:
      row.attachment && typeof row.attachment === 'object'
        ? {
            type: row.attachment.type,
            name: row.attachment.name,
            url: row.attachment.url,
            mime_type: row.attachment.mime_type ?? '',
            size_bytes:
              typeof row.attachment.size_bytes === 'number'
                ? row.attachment.size_bytes
                : null,
          }
        : null,
    dedupeKey: row.dedupe_key,
    timestamp: row.timestamp?.toISOString?.() ?? row.timestamp,
  });
}

export function mapUserRow(row) {
  if (!row) {
    return null;
  }
  return createUser({
    id: row.id,
    email: row.email,
    passwordHash: row.password_hash,
    role: row.role,
    displayName: row.display_name,
    tokenVersion: row.token_version,
    status: row.status,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
  });
}

export function mapRefreshTokenRow(row) {
  if (!row) {
    return null;
  }
  return createRefreshToken({
    id: row.id,
    userId: row.user_id,
    familyId: row.family_id,
    tokenHash: row.token_hash,
    expiresAt: row.expires_at?.toISOString?.() ?? row.expires_at,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
    revokedAt: row.revoked_at?.toISOString?.() ?? row.revoked_at,
    replacedByTokenId: row.replaced_by_token_id,
    tokenVersion: row.token_version,
  });
}

export function mapRecordingJobRow(row) {
  if (!row) {
    return null;
  }
  return createRecordingJob({
    id: row.id,
    classId: row.class_id,
    egressId: row.egress_id,
    rawRecordingPath: row.raw_recording_path,
    status: row.status,
    attempts: row.attempts,
    result: row.result,
    error: row.error,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
    updatedAt: row.updated_at?.toISOString?.() ?? row.updated_at,
  });
}
