# LalaCore AI Chat Website

This is the extracted chat-only web surface for the existing LalaCore AI module.

It uses the same backend contract as `lalacore_rebuild/lib/main.dart` and `AiEngineService`: `/app/action` with `ai_solve` / `ai_chat`, graph-of-thought, MCTS, verification, model priority, image input, PDF upload context, and AI chat history persistence.

## Railway

The FastAPI app serves this site at:

```text
https://your-railway-domain/ai-chat
```

When opened from `/ai-chat`, the site automatically calls the same Railway origin, so no extra CORS setting is needed.

## Local

If the backend is running locally:

```text
http://127.0.0.1:8000/ai-chat
```

You can also run only the static site:

```bash
python3 -m http.server 4173 --directory ai_chat_site
```

Then open:

```text
http://127.0.0.1:4173
```

When hosted separately, set the Backend URL in the settings panel. The current Railway backend is:

```text
https://web-production-75f40.up.railway.app
```
