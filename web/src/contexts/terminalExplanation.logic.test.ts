import assert from 'node:assert/strict';
import test from 'node:test';

import {
  initialTerminalExplanationState,
  reduceTerminalFrame,
  type TerminalFrame,
  type TerminalRender,
} from './terminalExplanation.logic.ts';

/** Fold a frame sequence, collecting everything the turn would render. */
function runFrames(frames: TerminalFrame[]) {
  let state = initialTerminalExplanationState();
  const rendered: TerminalRender[] = [];
  for (const frame of frames) {
    const result = reduceTerminalFrame(state, frame);
    state = result.state;
    if (result.render.kind !== 'none') rendered.push(result.render);
  }
  return { state, rendered };
}

const NOTICE =
  "*Turn stopped: the conversation exceeded the model's context window and could not be " +
  'reduced further. Start a new conversation or shorten the request.*';

// ── The bug this PR fixes ───────────────────────────────────────────────────

test('context exhaustion renders the localized notice on the live turn', () => {
  // Before the fix the gateway sent only `error`, so the mounted transcript
  // showed a generic provider-error bubble and the real reason appeared only
  // after a reload (#8758).
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    { type: 'error' },
  ]);

  assert.deepEqual(rendered, [{ kind: 'notice', content: NOTICE }]);
});

test('an explained turn renders exactly one terminal explanation', () => {
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    { type: 'error' },
  ]);

  assert.equal(rendered.length, 1, 'the generic error bubble must not restate the same stop');
  assert.deepEqual(rendered[0], { kind: 'notice', content: NOTICE });
});

// ── Negative controls: the suppression must not overreach ───────────────────

test('an ordinary failed turn still renders the generic error bubble', () => {
  const { rendered } = runFrames([{ type: 'turn_start' }, { type: 'error' }]);

  assert.deepEqual(rendered, [{ kind: 'error' }]);
});

test('a later unrelated failure is not swallowed by an earlier notice', () => {
  // Regression: a sticky flag would let turn 1's context-exhaustion notice
  // suppress turn 2's genuine error, leaving that turn silent.
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    { type: 'error' },
    { type: 'turn_start' },
    { type: 'error' },
  ]);

  assert.deepEqual(rendered, [{ kind: 'notice', content: NOTICE }, { kind: 'error' }]);
});

test('a notice with no following error does not leak into the next turn', () => {
  // The explanation is normally consumed by the error frame that follows it.
  // If that frame never arrives — dropped socket, or the user sends again
  // first — only the `turn_start` reset stops the stale flag from swallowing
  // the *next* turn's genuine error.
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    // no `error` for this turn
    { type: 'turn_start' },
    { type: 'error' },
  ]);

  assert.deepEqual(rendered, [{ kind: 'notice', content: NOTICE }, { kind: 'error' }]);
});

test('back-to-back error frames after one notice still surface the second', () => {
  // The explanation is consumed by the error it explains, not held open.
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    { type: 'error' },
    { type: 'error' },
  ]);

  assert.deepEqual(rendered, [{ kind: 'notice', content: NOTICE }, { kind: 'error' }]);
});

test('a notice frame with no text falls back to the generic error bubble', () => {
  // Defensive: a malformed/truncated frame must not silence the failure.
  const { rendered } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted' },
    { type: 'error' },
  ]);

  assert.deepEqual(rendered, [{ kind: 'error' }]);
});

test('state resets across turns so no explanation leaks forward', () => {
  const { state } = runFrames([
    { type: 'turn_start' },
    { type: 'context_exhausted', notice: NOTICE },
    { type: 'error' },
  ]);

  assert.equal(state.explained, false);
});
