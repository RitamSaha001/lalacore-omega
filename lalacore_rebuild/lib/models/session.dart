class Session {
  Session._();

  static bool isTeacher = false;
  static String studentId = '';
  static String studentName = '';
  static String accountId = '';
  static String email = '';
  static String username = '';
  static String chatId = '';
  static String chatName = '';

  static String get userRole => isTeacher ? 'teacher' : 'student';

  static String get userIdForNotifications {
    if (isTeacher) {
      return 'TEACHER';
    }
    if (accountId.isNotEmpty) {
      return accountId;
    }
    return studentId;
  }

  static String get effectiveAccountId {
    if (isTeacher) {
      return 'TEACHER';
    }
    if (accountId.isNotEmpty) {
      return accountId;
    }
    return studentId;
  }

  static String get effectiveChatId {
    if (isTeacher) {
      return 'TEACHER';
    }
    if (chatId.isNotEmpty) {
      return chatId;
    }
    return effectiveAccountId;
  }

  static String get effectiveDisplayName {
    if (chatName.isNotEmpty) {
      return chatName;
    }
    if (studentName.isNotEmpty) {
      return studentName;
    }
    return username;
  }
}
