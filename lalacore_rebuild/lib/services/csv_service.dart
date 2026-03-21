import 'package:csv/csv.dart';
import 'package:flutter/foundation.dart';

Future<List<List<dynamic>>> parseCsvInBackground(String data) {
  return compute((String d) => const CsvToListConverter().convert(d), data);
}
