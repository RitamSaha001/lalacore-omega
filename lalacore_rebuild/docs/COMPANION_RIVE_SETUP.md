# Companion Rive Setup

## 1) Artboard + Assets
1. Create a new Rive file and add artboard `Companion` at `1000 x 1000`.
2. Import all SVGs from `assets/companion/`.
3. Keep each layer centered in the same origin so animation pivots stay clean.

Layer tree:
```
Character
 ├ Body
 ├ Head
 │   ├ Hair
 │   ├ Eyes (open / closed)
 │   ├ Eyebrows (normal / playful)
 │   ├ Mouth (smile / open / flirty)
 │   └ NoseBubble
 └ Hands (left / right)
```

## 2) Animations
Create these animations with the exact names:

- `Idle` (loop 3s): subtle breathing (`translateY +5`), blink every ~2.5s.
- `Talking` (loop 0.5s): mouth smile/open swap.
- `Flirty` (0.8s): playful brow, flirty mouth, head tilt `+6deg`, quick wink.
- `Thinking` (1s): opposite head tilt, brows inward.
- `HappyJump` (0.7s): scale `1 -> 1.08 -> 1`, light vertical bounce.
- `Stretch` (1s): hands up, slight back arch.
- `Sleep` (loop): eyes closed, slow breathing, slight body tilt.
- `SnoreBubble` (loop): `nose_bubble` scale `0 -> 1 -> 0`.
- `Shocked` (0.4s): eyes `1 -> 1.2`, quick upward bounce.

Optional but recommended for GF toggle continuity:
- `Hello` trigger animation (short wave/pop-in).
- `Goodbye` trigger animation (small wave/pop-out).

## 3) State Machine
Create state machine `CompanionSM` with inputs:

- `SMIBool isTalking`
- `SMIBool isSleeping`
- `SMIBool showSnore`
- `SMITrigger flirty`
- `SMITrigger thinking`
- `SMITrigger happy`
- `SMITrigger stretch`
- `SMITrigger shocked`
- Optional: `SMITrigger hello`, `SMITrigger goodbye`

Priority behavior to enforce in transitions:
1. `Shocked` highest priority.
2. `Talking` next (`isTalking=true`).
3. Triggered interactions (`flirty/thinking/happy/stretch`).
4. Sleep (`isSleeping=true`).
5. `Idle` default.

Safety rules:
- `isSleeping=true` must force eyes closed / idle sleep pose.
- Block `SnoreBubble` when `isTalking=true`.
- Block talk while sleep is active unless controller wakes first.
- All trigger animations must auto-return to `Idle` on completion.

## 4) Export
1. Verify all input names exactly match the Flutter controller names.
2. Export file as `assets/companion/companion.riv`.
3. Run `flutter pub get` if assets changed and restart app.
