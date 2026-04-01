export class InMemoryDatabase {
  constructor() {
    this.classSessions = new Map();
    this.participantsByClass = new Map();
    this.breakoutRoomsByClass = new Map();
    this.chatMessagesByClass = new Map();
    this.chatDeduplicationByClass = new Map();
    this.idempotencyRecords = new Map();
    this.classOperationLogs = new Map();
    this.classSequences = new Map();
    this.clientInboundSequences = new Map();
    this.lockQueues = new Map();
    this.users = new Map();
    this.refreshTokens = new Map();
    this.recordingJobs = new Map();
  }

  ensureParticipants(classId) {
    if (!this.participantsByClass.has(classId)) {
      this.participantsByClass.set(classId, new Map());
    }
    return this.participantsByClass.get(classId);
  }

  ensureBreakoutRooms(classId) {
    if (!this.breakoutRoomsByClass.has(classId)) {
      this.breakoutRoomsByClass.set(classId, new Map());
    }
    return this.breakoutRoomsByClass.get(classId);
  }

  ensureChatMessages(classId) {
    if (!this.chatMessagesByClass.has(classId)) {
      this.chatMessagesByClass.set(classId, []);
    }
    return this.chatMessagesByClass.get(classId);
  }

  ensureChatDeduplication(classId) {
    if (!this.chatDeduplicationByClass.has(classId)) {
      this.chatDeduplicationByClass.set(classId, new Map());
    }
    return this.chatDeduplicationByClass.get(classId);
  }

  ensureOperationLog(classId) {
    if (!this.classOperationLogs.has(classId)) {
      this.classOperationLogs.set(classId, []);
    }
    return this.classOperationLogs.get(classId);
  }

  getLastSequence(classId) {
    return this.classSequences.get(classId) ?? 0;
  }

  appendOperation(classId, operation, { maxEntries = 2000 } = {}) {
    const sequenceNumber = this.getLastSequence(classId) + 1;
    this.classSequences.set(classId, sequenceNumber);
    const log = this.ensureOperationLog(classId);
    const entry = {
      ...operation,
      sequenceNumber,
    };
    log.push(entry);
    if (log.length > maxEntries) {
      log.splice(0, log.length - maxEntries);
    }
    return entry;
  }

  recordReplicatedOperation(classId, operation, { maxEntries = 2000 } = {}) {
    const current = this.getLastSequence(classId);
    this.classSequences.set(classId, Math.max(current, operation.sequenceNumber));
    const log = this.ensureOperationLog(classId);
    const exists = log.some(
      (entry) => entry.sequenceNumber === operation.sequenceNumber,
    );
    if (!exists) {
      log.push(operation);
      log.sort((left, right) => left.sequenceNumber - right.sequenceNumber);
      if (log.length > maxEntries) {
        log.splice(0, log.length - maxEntries);
      }
    }
    return operation;
  }

  getOperationsAfter(classId, sequenceNumber, channels = null) {
    return this.ensureOperationLog(classId).filter((entry) => {
      if (entry.sequenceNumber <= sequenceNumber) {
        return false;
      }
      if (!channels) {
        return true;
      }
      return channels.has(entry.channel);
    });
  }

  acceptClientSequence(clientKey, sequenceNumber) {
    const last = this.clientInboundSequences.get(clientKey) ?? 0;
    if (sequenceNumber <= last) {
      return {
        accepted: false,
        lastProcessedSequence: last,
      };
    }
    this.clientInboundSequences.set(clientKey, sequenceNumber);
    return {
      accepted: true,
      lastProcessedSequence: sequenceNumber,
    };
  }

  async withLocks(lockKeys, callback) {
    const keys = [...new Set(lockKeys)].sort();
    const releases = [];

    for (const key of keys) {
      // Sorted lock acquisition keeps cross-entity operations deadlock-free.
      const previousTail = this.lockQueues.get(key) ?? Promise.resolve();
      let release;
      const nextTail = new Promise((resolve) => {
        release = resolve;
      });
      const currentTail = previousTail.then(() => nextTail);
      this.lockQueues.set(key, currentTail);
      await previousTail;
      releases.push(() => {
        release();
        if (this.lockQueues.get(key) === currentTail) {
          this.lockQueues.delete(key);
        }
      });
    }

    try {
      return await callback();
    } finally {
      for (const release of releases.reverse()) {
        release();
      }
    }
  }

  async healthCheck() {
    return {
      component: 'database',
      driver: 'memory',
      status: 'ready',
    };
  }

  async close() {}
}
