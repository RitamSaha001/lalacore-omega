export class ReconnectionService {
  constructor({
    config,
    database,
    participantRepository,
    logger,
    onPresenceChanged,
  }) {
    this.config = config;
    this.database = database;
    this.participantRepository = participantRepository;
    this.logger = logger;
    this.onPresenceChanged = onPresenceChanged;
    this.activeConnections = new Map();
    this.disconnectTimers = new Map();
    this.sweepTimer = null;
  }

  participantKey(classId, userId) {
    return `${classId}:${userId}`;
  }

  start() {
    if (this.sweepTimer) {
      return;
    }
    const intervalMs = Math.max(
      1000,
      Number(this.config.reconnectSweepIntervalMs || 5000),
    );
    this.sweepTimer = setInterval(() => {
      void this.sweepExpiredDisconnects();
    }, intervalMs);
    if (typeof this.sweepTimer.unref === 'function') {
      this.sweepTimer.unref();
    }
  }

  async close() {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    for (const timer of this.disconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectTimers.clear();
  }

  async registerConnection({ classId, userId, connectionId }) {
    if (!userId) {
      return null;
    }

    const key = this.participantKey(classId, userId);
    if (!this.activeConnections.has(key)) {
      this.activeConnections.set(key, new Set());
    }
    this.activeConnections.get(key).add(connectionId);

    const timer = this.disconnectTimers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.disconnectTimers.delete(key);
    }

    let changedPresence = false;
    const participant = await this.database.withLocks(
      [`participants:${classId}`],
      async () => {
        const current = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!current) {
          return null;
        }
        if (current.presenceStatus === 'connected') {
          return this.participantRepository.updateState(classId, userId, {
            lastSeenAt: new Date().toISOString(),
          });
        }
        changedPresence = true;
        return this.participantRepository.updateState(classId, userId, {
          presenceStatus: 'connected',
          lastSeenAt: new Date().toISOString(),
          disconnectedAt: null,
          disconnectGraceExpiresAt: null,
        });
      },
    );

    if (participant) {
      this.logger.info('participant_reconnected', {
        classId,
        userId,
        connectionId,
      });
      if (changedPresence) {
        await this.onPresenceChanged?.(classId, participant);
      }
    }

    return participant;
  }

  async unregisterConnection({ classId, userId, connectionId }) {
    if (!userId) {
      return null;
    }

    const key = this.participantKey(classId, userId);
    const connections = this.activeConnections.get(key);
    if (connections) {
      connections.delete(connectionId);
      if (connections.size === 0) {
        this.activeConnections.delete(key);
      }
    }

    if ((this.activeConnections.get(key)?.size ?? 0) > 0) {
      return null;
    }

    const existingTimer = this.disconnectTimers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
      this.disconnectTimers.delete(key);
    }

    const graceDeadline = new Date(
      Date.now() + this.config.reconnectGracePeriodMs,
    ).toISOString();
    let requiresGraceTimer = false;

    const participant = await this.database.withLocks(
      [`participants:${classId}`],
      async () => {
        const current = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!current) {
          return null;
        }
        if (current.status !== 'approved') {
          return this.participantRepository.updateState(classId, userId, {
            presenceStatus: 'offline',
            disconnectedAt: new Date().toISOString(),
            disconnectGraceExpiresAt: null,
          });
        }
        requiresGraceTimer = true;
        return this.participantRepository.updateState(classId, userId, {
          presenceStatus: 'temporarily_disconnected',
          disconnectedAt: new Date().toISOString(),
          disconnectGraceExpiresAt: graceDeadline,
        });
      },
    );

    if (!participant) {
      return null;
    }

    this.logger.warn(
      requiresGraceTimer
        ? 'participant_temporarily_disconnected'
        : 'participant_offline',
      {
        classId,
        userId,
        graceDeadline: requiresGraceTimer ? graceDeadline : null,
      },
    );
    await this.onPresenceChanged?.(classId, participant);

    if (requiresGraceTimer) {
      const timer = setTimeout(() => {
        void this.finalizeDisconnect({
          classId,
          userId,
          expectedGraceDeadline: graceDeadline,
        });
      }, this.config.reconnectGracePeriodMs);
      this.disconnectTimers.set(key, timer);
    }
    return participant;
  }

  async finalizeDisconnect({ classId, userId, expectedGraceDeadline }) {
    const key = this.participantKey(classId, userId);
    this.disconnectTimers.delete(key);
    if ((this.activeConnections.get(key)?.size ?? 0) > 0) {
      return null;
    }

    const participant = await this.database.withLocks(
      [`participants:${classId}`],
      async () => {
        const current = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (
          !current ||
          current.presenceStatus !== 'temporarily_disconnected' ||
          current.disconnectGraceExpiresAt !== expectedGraceDeadline
        ) {
          return null;
        }

        return this.participantRepository.updateState(classId, userId, {
          presenceStatus: 'left',
        });
      },
    );

    if (!participant) {
      return null;
    }

    this.logger.warn('participant_disconnect_grace_expired', {
      classId,
      userId,
    });
    await this.onPresenceChanged?.(classId, participant);
    return participant;
  }

  async sweepExpiredDisconnects() {
    const nowIso = new Date().toISOString();
    const expired = await this.participantRepository.listExpiredTemporaryDisconnects(
      nowIso,
      { limit: 200 },
    );
    for (const participant of expired) {
      if (!participant?.classId || !participant?.userId) {
        continue;
      }
      try {
        await this.finalizeDisconnect({
          classId: participant.classId,
          userId: participant.userId,
          expectedGraceDeadline: participant.disconnectGraceExpiresAt,
        });
      } catch (error) {
        this.logger.warn('participant_disconnect_sweep_failed', {
          classId: participant.classId,
          userId: participant.userId,
          error: String(error),
        });
      }
    }
  }
}
