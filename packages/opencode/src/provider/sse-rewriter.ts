import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "provider-sse-rewriter" })

type SseEventPayload = Record<string, any>

/**
 * Rewrite mismatched `item_id` values in Responses-API SSE streams.
 *
 * Some OpenAI-compatible proxies (notably Databricks AI Gateway)
 * re-encode Responses-API SSE and emit different ids between
 * `response.output_item.added` (item.id) and the subsequent
 * `response.output_text.delta` / `response.content_part.*` events
 * (item_id), which breaks @ai-sdk/openai's stream parser with
 * "text part <id> not found". Both events carry the same `output_index`,
 * so we use it as the correlation key.
 *
 * Only message-type items are cached. Other item types (`reasoning`,
 * `tool_call`, etc.) are intentionally ignored — they have separate
 * downstream parsers and don't trigger the bug. Refs opencode #25487.
 */
export function wrapResponsesSseStream(
  res: Response,
  ctx: { url: string; npm: string; rawSse?: boolean; providerID: string; modelID: string },
): Response {
  if (ctx.rawSse) return res
  if (ctx.npm !== "@ai-sdk/openai") return res
  if (!ctx.url.includes("/responses")) return res
  if (!res.headers.get("content-type")?.includes("text/event-stream")) return res
  if (!res.body) return res

  const decoder = new TextDecoder()
  const encoder = new TextEncoder()
  const canonical = new Map<number, string>()
  const warned = new Set<string>()
  let buffer = ""

  const rewriteEvent = (eventText: string): string => {
    const lines = eventText.split("\n")
    let mutated = false
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i]
      if (!line.startsWith("data:")) continue
      const trimmed = line.slice(5).trim()
      if (!trimmed || trimmed === "[DONE]") continue
      let payload: SseEventPayload | undefined
      try {
        payload = JSON.parse(trimmed) as SseEventPayload
      } catch {
        continue
      }
      if (!payload || typeof payload !== "object") continue
      if (
        payload.type === "response.output_item.added" &&
        payload.item?.type === "message" &&
        typeof payload.item.id === "string" &&
        typeof payload.output_index === "number"
      ) {
        canonical.set(payload.output_index, payload.item.id)
        continue
      }
      if (payload.type === "response.output_item.done" && typeof payload.output_index === "number") {
        canonical.delete(payload.output_index)
        continue
      }
      if (
        typeof payload.output_index === "number" &&
        typeof payload.item_id === "string" &&
        canonical.has(payload.output_index)
      ) {
        const want = canonical.get(payload.output_index)!
        if (want !== payload.item_id) {
          const dedupeKey = JSON.stringify([payload.output_index, payload.item_id])
          if (!warned.has(dedupeKey)) {
            warned.add(dedupeKey)
            log.warn("responses_sse_id_rewrite", {
              providerID: ctx.providerID,
              modelID: ctx.modelID,
              output_index: payload.output_index,
              originalId: payload.item_id,
              canonicalId: want,
              sequence_number: payload.sequence_number,
            })
          }
          payload.item_id = want
          lines[i] = "data: " + JSON.stringify(payload)
          mutated = true
        }
      }
    }
    return mutated ? lines.join("\n") : eventText
  }

  const reader = res.body.getReader()
  const stream = new ReadableStream<Uint8Array>({
    async pull(ctrl) {
      // Loop until we either close or enqueue something. Returning from pull()
      // without enqueueing on a backpressured consumer can stall the read.
      while (true) {
        const part = await reader.read()
        if (part.done) {
          if (buffer.length > 0) {
            ctrl.enqueue(encoder.encode(buffer))
            buffer = ""
          }
          ctrl.close()
          return
        }
        buffer += decoder.decode(part.value, { stream: true })
        const out: string[] = []
        let idx: number
        while ((idx = buffer.indexOf("\n\n")) !== -1) {
          const event = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)
          const transformed = rewriteEvent(event)
          out.push(transformed, "\n\n")
        }
        if (out.length > 0) {
          ctrl.enqueue(encoder.encode(out.join("")))
          return
        }
      }
    },
    async cancel(reason) {
      await reader.cancel(reason)
    },
  })

  // Re-streaming invalidates any upstream Content-Length; strip it so
  // downstream consumers don't truncate / mis-parse the body.
  const headers = new Headers(res.headers)
  headers.delete("content-length")
  return new Response(stream, {
    headers,
    status: res.status,
    statusText: res.statusText,
  })
}
