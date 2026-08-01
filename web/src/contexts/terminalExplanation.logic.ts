/** Terminal-explanation arbitration for a single turn.
 *
 *  A turn that dies on unrecoverable context exhaustion produces two gateway
 *  frames: the `context_exhausted` notice (localized, explains *why* the turn
 *  stopped) and the generic `error` frame that always follows a failed turn.
 *  Rendering both leaves the transcript restating one stop twice — once in
 *  operator wording, once in raw provider wording.
 *
 *  This module owns that decision so it is testable at the frame boundary
 *  rather than living as untestable ref mutations inside the context provider.
 *  See #8758.
 */

/** Frames that can change which terminal explanation a turn renders. */
export type TerminalFrame =
  | { type: 'turn_start' }
  | { type: 'context_exhausted'; notice?: string }
  | { type: 'error' };

export interface TerminalExplanationState {
  /** A context-exhaustion notice already explained the in-flight turn. */
  explained: boolean;
}

/** What the handler should render for this frame. */
export type TerminalRender =
  | { kind: 'none' }
  /** The localized context-exhaustion notice bubble. */
  | { kind: 'notice'; content: string }
  /** The generic error bubble. */
  | { kind: 'error' };

export function initialTerminalExplanationState(): TerminalExplanationState {
  return { explained: false };
}

export function reduceTerminalFrame(
  state: TerminalExplanationState,
  frame: TerminalFrame,
): { state: TerminalExplanationState; render: TerminalRender } {
  switch (frame.type) {
    case 'turn_start':
      // A new turn must never inherit the previous turn's explanation, or an
      // unrelated later failure would render silently.
      return { state: { explained: false }, render: { kind: 'none' } };
    case 'context_exhausted':
      // A frame with no notice text carries nothing to render; leave the turn
      // unexplained so the error frame still surfaces the failure.
      if (!frame.notice) return { state, render: { kind: 'none' } };
      return { state: { explained: true }, render: { kind: 'notice', content: frame.notice } };
    case 'error':
      if (state.explained) {
        // Consume the explanation: this turn is over.
        return { state: { explained: false }, render: { kind: 'none' } };
      }
      return { state, render: { kind: 'error' } };
  }
}
