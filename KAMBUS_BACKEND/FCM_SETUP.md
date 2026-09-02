# Firebase Cloud Messaging setup

1. In Firebase Console, add Android app `com.example.kambus` and place the downloaded `google-services.json` in `C:\Users\Rehan\AndroidStudioProjects\KAMBUS\app\`.
2. Enable the Google Services Gradle plugin if you use the generated configuration.
3. Install `firebase-admin` in the backend virtual environment.
4. Set `FCM_SERVICE_ACCOUNT_FILE` to the absolute path of the Firebase Admin service-account JSON before starting FastAPI. Do not commit that file or place it in Android assets.

Without these settings KAMBUS still stores and displays in-app notifications; push delivery is deliberately skipped safely.
