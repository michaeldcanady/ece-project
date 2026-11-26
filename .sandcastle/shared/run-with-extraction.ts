import { run } from "@ai-hero/sandcastle";
import type {
  OutputObjectDefinition,
  RunOptions,
  RunResult,
} from "@ai-hero/sandcastle";

export interface RunWithExtractionOptions<T>
  extends Omit<RunOptions, "output"> {
  readonly output: OutputObjectDefinition<T>;
  readonly extractionPrompt: string;
  /** Extra attempts after the first if extraction or validation fails. */
  readonly maxRetries?: number;
}

/**
 * Run the long producing session, then resume it with a short prompt that
 * emits only the structured output tag. Adapted from the Sandcastle repo's
 * own agent workflows (MIT).
 */
export const runWithExtraction = async <T>(
  options: RunWithExtractionOptions<T>,
): Promise<RunResult & { output: T }> => {
  const { output, extractionPrompt, maxRetries = 2, ...produceOptions } = options;
  const produce = await run(produceOptions);
  const sessionId = produce.iterations.at(-1)?.sessionId;

  if (sessionId === undefined || sessionId === "") {
    throw new Error(
      "Cannot extract structured output because the produce run had no session id.",
    );
  }

  const { promptArgs: _promptArgs, ...extractionOptions } = produceOptions;
  const extraction = await run({
    ...extractionOptions,
    name:
      produceOptions.name !== undefined && produceOptions.name !== ""
        ? `${produceOptions.name} (extract)`
        : undefined,
    promptFile: undefined,
    prompt: extractionPrompt,
    completionSignal: `</${output.tag}>`,
    resumeSession: sessionId,
    output: { ...output, maxRetries },
  });

  return { ...produce, output: extraction.output };
};
