import { describe, expect, test } from "bun:test"
import { wrapResponsesSseStream } from "@/provider/sse-rewriter"

const SSE = "text/event-stream"

function makeRes(body: string | ReadableStream<Uint8Array>, ct = SSE) {
  return new Response(body, { headers: { "content-type": ct } })
}

async function readAll(res: Response): Promise<string> {
  const reader = res.body!.getReader()
  const decoder = new TextDecoder()
  let out = ""
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    out += decoder.decode(value, { stream: true })
  }
  out += decoder.decode()
  return out
}

const ctx = (overrides: Partial<{ url: string; npm: string; rawSse: boolean }> = {}) => ({
  url: "https://api.example.com/openai/v1/responses",
  npm: "@ai-sdk/openai",
  rawSse: false,
  providerID: "test",
  modelID: "test-model",
  ...overrides,
})

describe("wrapResponsesSseStream", () => {
  test("(a) rewrites mismatched item_id using output_index from output_item.added", async () => {
    const body = [
      "event: response.output_item.added",
      `data: ${JSON.stringify({ type: "response.output_item.added", output_index: 1, item: { type: "message", id: "msg_short" } })}`,
      "",
      "event: response.output_text.delta",
      `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 1, item_id: "msg_LONG_id", delta: "hello" })}`,
      "",
      "",
    ].join("\n")
    const res = wrapResponsesSseStream(makeRes(body), ctx())
    const text = await readAll(res)
    expect(text).toContain('"item_id":"msg_short"')
    expect(text).not.toContain('"item_id":"msg_LONG_id"')
    expect(text).toContain('"delta":"hello"')
  })

  test("(b) native conforming stream (ids match) is unchanged", async () => {
    const body = [
      "event: response.output_item.added",
      `data: ${JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { type: "message", id: "msg_x" } })}`,
      "",
      "event: response.output_text.delta",
      `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: "msg_x", delta: "hi" })}`,
      "",
      "",
    ].join("\n")
    const res = wrapResponsesSseStream(makeRes(body), ctx())
    const text = await readAll(res)
    expect(text).toBe(body)
  })

  test("(c) chunk-boundary mid-event split still rewrites correctly", async () => {
    const event1 =
      "event: response.output_item.added\n" +
      "data: " +
      JSON.stringify({
        type: "response.output_item.added",
        output_index: 1,
        item: { type: "message", id: "msg_short" },
      }) +
      "\n\n"
    const event2 =
      "event: response.output_text.delta\n" +
      "data: " +
      JSON.stringify({
        type: "response.output_text.delta",
        output_index: 1,
        item_id: "msg_LONG_id",
        delta: "yo",
      }) +
      "\n\n"
    const all = event1 + event2
    const splitAt = event1.length + Math.floor(event2.length / 2)
    const enc = new TextEncoder()
    const stream = new ReadableStream<Uint8Array>({
      start(ctrl) {
        ctrl.enqueue(enc.encode(all.slice(0, splitAt)))
        ctrl.enqueue(enc.encode(all.slice(splitAt)))
        ctrl.close()
      },
    })
    const res = wrapResponsesSseStream(makeRes(stream), ctx())
    const text = await readAll(res)
    expect(text).toContain('"item_id":"msg_short"')
    expect(text).not.toContain('"item_id":"msg_LONG_id"')
  })

  test("(d) output_index reuse after item.done uses fresh canonical id", async () => {
    const body = [
      `data: ${JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { type: "message", id: "msg_a" } })}`,
      "",
      `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: "wrong1", delta: "first" })}`,
      "",
      `data: ${JSON.stringify({ type: "response.output_item.done", output_index: 0, item: { type: "message", id: "msg_a" } })}`,
      "",
      `data: ${JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { type: "message", id: "msg_b" } })}`,
      "",
      `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: "wrong2", delta: "second" })}`,
      "",
      "",
    ].join("\n")
    const res = wrapResponsesSseStream(makeRes(body), ctx())
    const text = await readAll(res)
    expect(text).toMatch(/"item_id":"msg_a"[^\n]*"delta":"first"/)
    expect(text).toMatch(/"item_id":"msg_b"[^\n]*"delta":"second"/)
    expect(text).not.toContain("wrong1")
    expect(text).not.toContain("wrong2")
  })

  test("(e) reordered delta-before-added passes through unmodified", async () => {
    const body = [
      `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: "msg_orphan", delta: "before-added" })}`,
      "",
      `data: ${JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { type: "message", id: "msg_real" } })}`,
      "",
      "",
    ].join("\n")
    const res = wrapResponsesSseStream(makeRes(body), ctx())
    const text = await readAll(res)
    expect(text).toContain('"item_id":"msg_orphan"')
  })

  test("(f) rawSse opt-out returns original Response unchanged", () => {
    const original = makeRes("data: {}\n\n")
    const res = wrapResponsesSseStream(original, ctx({ rawSse: true }))
    expect(res).toBe(original)
  })

  test("gate: non-openai npm returns original Response unchanged", () => {
    const original = makeRes("data: {}\n\n")
    const res = wrapResponsesSseStream(original, ctx({ npm: "@ai-sdk/openai-compatible" }))
    expect(res).toBe(original)
  })

  test("gate: URL not containing /responses returns original Response unchanged", () => {
    const original = makeRes("data: {}\n\n")
    const res = wrapResponsesSseStream(original, ctx({ url: "https://api.example.com/openai/v1/chat/completions" }))
    expect(res).toBe(original)
  })

  test("gate: non-SSE content-type returns original Response unchanged", () => {
    const original = makeRes("{}", "application/json")
    const res = wrapResponsesSseStream(original, ctx())
    expect(res).toBe(original)
  })
})
