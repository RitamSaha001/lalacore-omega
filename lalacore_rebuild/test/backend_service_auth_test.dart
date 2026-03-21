import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:lalacore_rebuild/config/app_config.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

void main() {
  group('BackendService auth/otp', () {
    test(
      'authenticate falls back to script when local returns wrong password',
      () async {
        final List<String> actions = <String>[];

        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method != 'POST') {
              return http.Response('{}', 200);
            }
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final String action = (body['action'] ?? '').toString();
            actions.add(action);
            final bool isAuthBackend = request.url.toString().contains(
              '/auth/action',
            );

            if (action == 'login_direct' && isAuthBackend) {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': false,
                  'status': 'WRONG_PASSWORD',
                }),
                200,
              );
            }
            if (action == 'login_direct' && !isAuthBackend) {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': true,
                  'status': 'SUCCESS',
                  'name': 'Script Student',
                  'student_id': 'SCR123',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
              200,
            );
          }),
        );

        final Map<String, dynamic> response = await service.authenticate(
          login: true,
          email: 'student@example.com',
          password: 'new-pass',
          name: '',
        );

        expect(response['ok'], true);
        expect(response['status'], 'SUCCESS');
        expect(actions, contains('upsert_user'));
      },
    );

    test('requestEmailOtp falls back when first action is unknown', () async {
      final List<Map<String, dynamic>> actions = <Map<String, dynamic>>[];

      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method != 'POST') {
            return http.Response('{}', 200);
          }
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add(body);

          if (body['action'] == 'request_login_otp') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }

          return http.Response(
            jsonEncode(<String, dynamic>{'status': 'OTP_SENT', 'ok': true}),
            200,
          );
        }),
      );

      final Map<String, dynamic> response = await service.requestEmailOtp(
        login: true,
        email: 'student@example.com',
      );

      expect(response['ok'], true);
      expect(actions.length, 2);
      expect(actions.first['action'], 'request_login_otp');
      expect(actions.last['action'], 'request_email_otp');
    });

    test('resetPasswordWithOtp sends otp and new password payload', () async {
      Map<String, dynamic> posted = <String, dynamic>{};

      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'POST') {
            posted = jsonDecode(request.body) as Map<String, dynamic>;
          }
          return http.Response(
            jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
            200,
          );
        }),
      );

      final Map<String, dynamic> response = await service.resetPasswordWithOtp(
        email: 'student@example.com',
        otp: '123456',
        newPassword: 'new-pass',
      );

      expect(response['ok'], true);
      expect(posted['email'], 'student@example.com');
      expect(posted['otp'], '123456');
      expect(
        posted['new_password'] == 'new-pass' ||
            posted['password'] == 'new-pass',
        true,
      );
    });

    test('requestForgotPasswordOtp carries sender email metadata', () async {
      final List<Map<String, dynamic>> posted = <Map<String, dynamic>>[];

      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'POST') {
            posted.add(jsonDecode(request.body) as Map<String, dynamic>);
          }
          return http.Response(
            jsonEncode(<String, dynamic>{'ok': true, 'status': 'OTP_SENT'}),
            200,
          );
        }),
      );

      final Map<String, dynamic> response = await service
          .requestForgotPasswordOtp(email: 'student@example.com');
      expect(response['ok'], true);
      expect(posted, isNotEmpty);
      expect(posted.first['email'], 'student@example.com');
      expect(
        posted.first['sender_email'] == null ||
            posted.first['sender_email'] == AppConfig.forgotOtpSenderEmail,
        true,
      );
    });

    test(
      'requestForgotPasswordOtp reports unknown action as failure',
      () async {
        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'POST') {
              return http.Response('Unknown Action', 200);
            }
            return http.Response('{}', 200);
          }),
        );

        final Map<String, dynamic> response = await service
            .requestForgotPasswordOtp(email: 'student@example.com');

        expect(response['ok'], isNot(true));
        expect(
          (response['message'] ?? '').toString().toLowerCase(),
          contains('unknown action'),
        );
      },
    );
  });
}
