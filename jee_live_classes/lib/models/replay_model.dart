import 'lecture_index_model.dart';
import 'transcript_model.dart';

class ReplayModel {
  const ReplayModel({
    required this.classId,
    required this.videoUrl,
    required this.transcript,
    required this.conceptIndex,
  });

  final String classId;
  final String videoUrl;
  final List<TranscriptModel> transcript;
  final List<LectureIndexModel> conceptIndex;
}
