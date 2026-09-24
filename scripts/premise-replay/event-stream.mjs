// The stream supplies hints only. Coalesce bursts, but repeat the read if a hint
// arrives during an outstanding read; otherwise the last change could be lost.
export class EventStream {
  constructor({ refresh, event, error, closed }) {
    Object.assign(this, { refresh, event, error, closed });
    this.session = null;
  }

  async connect(url, headers) {
    await this.disconnect();
    const session = { abort: new AbortController(), dirty: false, reading: null };
    const timer = setTimeout(() => session.abort.abort(), 10000);
    let response;
    try {
      response = await fetch(url, { headers, signal: session.abort.signal });
    } finally {
      clearTimeout(timer);
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`sse ${response.status}`);
    }
    this.session = session;
    session.pump = this.read(response.body.getReader(), session);
    // Even an empty stream must recover state missed before this connection.
    await this.requestRefresh(session);
  }

  requestRefresh(session) {
    session.dirty = true;
    if (!session.reading) {
      session.reading = (async () => {
        while (session.dirty && this.session === session) {
          session.dirty = false;
          await this.refresh();
        }
      })().catch((error) => this.error(error)).finally(() => { session.reading = null; });
    }
    return session.reading;
  }

  async read(reader, session) {
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      while (this.session === session) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let match;
        while ((match = /\r?\n\r?\n/.exec(buffer))) {
          const frame = buffer.slice(0, match.index);
          buffer = buffer.slice(match.index + match[0].length);
          const kind = frame.match(/^event: ?([^\r\n]*)/m)?.[1];
          if (!kind || this.session !== session) continue;
          const data = frame.match(/^data: ?([^\r\n]*)/m)?.[1];
          this.event({ kind, data });
          void this.requestRefresh(session);
        }
      }
    } catch (error) {
      if (error.name !== "AbortError") this.error(error);
    } finally {
      reader.releaseLock();
      if (this.session === session) {
        this.closed();
      }
    }
  }

  async disconnect() {
    const session = this.session;
    if (!session) return;
    this.session = null;
    session.abort.abort();
    await session.pump;
    await session.reading;
    this.closed();
  }
}
