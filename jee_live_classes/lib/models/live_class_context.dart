class LiveClassContext {
  const LiveClassContext({
    required this.userId,
    required this.userName,
    required this.role,
    required this.classId,
    required this.sessionToken,
    required this.classTitle,
    required this.subject,
    required this.topic,
    required this.teacherName,
    this.className,
    this.startTimeLabel,
  });

  final String userId;
  final String userName;
  final String role;
  final String classId;
  final String sessionToken;
  final String classTitle;
  final String subject;
  final String topic;
  final String teacherName;
  final String? className;
  final String? startTimeLabel;

  bool get isTeacher {
    final normalized = role.toLowerCase();
    return normalized == 'teacher' ||
        normalized == 'host' ||
        normalized == 'cohost' ||
        normalized == 'co_host';
  }
}
