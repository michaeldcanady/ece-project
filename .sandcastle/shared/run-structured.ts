import { run } from "@ai-hero/sandcastle";
import type { OutputObjectDefinition, RunOptions, RunResult } from "@ai-hero/sandcastle";

export interface RunStructuredOptions<T> extends Omit<RunOptions, "output" | "completionSignal"> {
  readonly output: OutputObjectDefinition<T>;
  /** Extra attempts after the first if the output tag is missing or fails validation. */
  readonly maxRetries?: number;
}

/**
 * Run one session that ends with the structured output tag, which the prompt's OUTPUT_SHAPE section describes.
 * Sandcastle resumes the session to correct a missing or invalid tag, up to `maxRetries` times, so the common case
 * costs one session instead of a produce session plus an extraction session that re-emits the whole answer.
 */
export const runStructured = async <T>(options: RunStructuredOptions<T>): Promise<RunResult & { output: T }> => {
  const { output, maxRetries = 2, ...runOptions } = options;
  return run({ ...runOptions, completionSignal: `</${output.tag}>`, output: { ...output, maxRetries } });
};
