/** The label names the Factory's workflows and scripts share. `setup.sh` creates them; `check_agent_labels.py` checks that. */

/** A human filed the issue from the Request template; it outranks the nightly scan's candidates. */
export const REQUEST_LABEL = "request";
/** Jumps the implement queue. */
export const PRIORITY_HIGH_LABEL = "priority:high";
/** Asks for a spec run. */
export const SPEC_LABEL = "agent:spec";
/** Puts the issue in the implement queue. */
export const IMPLEMENT_LABEL = "agent:implement";
/** The issue's one recovery attempt has been used. */
export const RECOVERED_LABEL = "agent:recovered";
export const BLOCKED_LABEL = "agent:blocked";
export const DOOR_ONE_WAY_LABEL = "door:one-way";
export const DOOR_TWO_WAY_LABEL = "door:two-way";
