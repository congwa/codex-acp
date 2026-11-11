import type {WindowSnapshot} from "./WindowSnapshot";
import {record, type RecordValue, WindowRequestError, WindowTransport} from "./WindowTransport";

const FOLLOW = "thread-stream-following-changed";
const STATE = "thread-stream-state-changed";

export function applyWindowPatches(state: RecordValue, patches: unknown[]): RecordValue {
    const next = structuredClone(state);
    for (const value of patches) {
        const patch = record(value);
        const raw = patch?.["path"];
        const op = patch?.["op"];
        const path = Array.isArray(raw) ? raw.map(String)
            : typeof raw === "string" && raw.startsWith("/") ? raw.slice(1).split("/").map(x => x.replace(/~1/g, "/").replace(/~0/g, "~")) : [];
        if (!path.length || path.some(x => ["__proto__", "constructor", "prototype"].includes(x))
            || !["add", "remove", "replace"].includes(String(op))) throw new Error("Invalid window state patch");
        let parent: unknown = next;
        for (const part of path.slice(0, -1)) {
            if (parent === null || typeof parent !== "object" || !Object.hasOwn(parent, part)) throw new Error("Missing window state patch parent");
            parent = (parent as RecordValue)[part];
        }
        const key = path.at(-1)!;
        if (Array.isArray(parent)) {
            const index = key === "-" && op === "add" ? parent.length : /^\d+$/.test(key) ? Number(key) : -1;
            if (index < 0 || index > parent.length || (op !== "add" && index === parent.length)) throw new Error("Invalid window state patch index");
            if (op === "remove") parent.splice(index, 1);
            else if (op === "add") parent.splice(index, 0, patch!["value"]);
            else parent[index] = patch!["value"];
        } else {
            const target = record(parent);
            if (!target || (op !== "add" && !Object.hasOwn(target, key))) throw new Error("Missing window state patch target");
            if (op === "remove") delete target[key];
            else target[key] = patch!["value"];
        }
    }
    return next;
}

export class WindowFollower {
    private transport: WindowTransport | undefined;
    private owner: string | undefined;
    private current: WindowSnapshot | undefined;
    private closed = false;
    private generation = 0;
    private opening: Promise<WindowSnapshot | null> | undefined;
    private readonly listeners = new Set<() => void>();
    private readonly target: RecordValue;

    constructor(private readonly path: string, threadId: string, private readonly changed: (snapshot: WindowSnapshot | undefined) => void) {
        this.target = {conversationId: threadId, hostId: "local"};
    }

    get epoch(): number { return this.generation; }
    get snapshot(): WindowSnapshot | undefined { return this.current; }

    open(): Promise<WindowSnapshot | null> {
        if (this.closed) return Promise.reject(new Error("Codex window follower is closed"));
        if (this.current) return Promise.resolve(this.current);
        this.opening ??= this.connect().finally(() => { this.opening = undefined; });
        return this.opening;
    }

    async control(method: string, version: number, params: RecordValue, epoch = this.epoch): Promise<unknown> {
        const transport = this.transport;
        if (epoch !== this.epoch || !this.current || !transport || !this.owner) {
            throw new WindowRequestError("owner-unavailable", false);
        }
        try {
            return (await transport.request(method, version, {...params, conversationId: this.target["conversationId"]}, this.owner))["result"];
        } catch (error) {
            if (epoch === this.epoch && error instanceof WindowRequestError
                && (error.reason === "no-client-found" || error.reason === "client-disconnected")) transport.close();
            throw error;
        }
    }

    async waitFor<T>(select: () => T | undefined, timeoutMs: number, signal?: AbortSignal): Promise<T> {
        return await new Promise<T>((resolve, reject) => {
            const listener = () => {
                try {
                    const value = select();
                    if (value !== undefined) { cleanup(); resolve(value); }
                    else if (this.closed || signal?.aborted) abort();
                } catch (error) {
                    cleanup();
                    reject(error);
                }
            };
            const abort = () => { cleanup(); reject(new WindowRequestError("observation-cancelled", true)); };
            const timer = setTimeout(() => { cleanup(); reject(new WindowRequestError("observation-timeout", true)); }, timeoutMs);
            const cleanup = () => {
                clearTimeout(timer);
                this.listeners.delete(listener);
                signal?.removeEventListener("abort", abort);
            };
            this.listeners.add(listener);
            signal?.addEventListener("abort", abort, {once: true});
            listener();
        });
    }

    close(): void {
        this.closed = true;
        if (this.owner && this.transport) this.follow(false);
        this.transport?.close();
        this.reset();
    }

    private async connect(): Promise<WindowSnapshot | null> {
        this.transport?.close();
        const epoch = ++this.generation;
        try {
            const transport = await WindowTransport.connect(this.path, event => {
                if (epoch === this.generation && !this.closed) this.receive(event);
            }, () => {
                if (epoch === this.generation) this.reset();
            });
            if (this.closed || epoch !== this.generation) {
                transport.close();
                throw new Error("Codex window connection was superseded");
            }
            this.transport = transport;
            const discovery = await transport.request("thread-owner-discovery", 1, this.target);
            const owner = discovery["handledByClientId"];
            if (typeof owner !== "string") throw new Error("Codex window IPC did not identify the owner");
            this.owner = owner;
            this.follow(true);
            return await this.waitFor(() => {
                if (epoch !== this.generation) throw new WindowRequestError("owner-disconnected", true);
                return this.current;
            }, 6000);
        } catch (error) {
            if (epoch === this.generation) this.transport?.close();
            if (error instanceof WindowRequestError && error.reason === "no-client-found") return null;
            throw error;
        }
    }

    private follow(following: boolean): void {
        if (this.owner) this.transport?.broadcast(FOLLOW, 1, {...this.target, following}, [this.owner]);
    }

    private receive(event: RecordValue): void {
        const params = record(event["params"]);
        if (event["method"] === "ipc-connection-reset") {
            this.transport?.close();
            return;
        }
        if (event["method"] === "client-status-changed" && params?.["clientId"] === this.owner && params?.["status"] === "disconnected") {
            this.transport?.close();
            return;
        }
        if (event["sourceClientId"] !== this.owner || params?.["conversationId"] !== this.target["conversationId"]
            || params?.["hostId"] !== "local") return;
        if (event["method"] === "thread-stream-following-status-requested") {
            this.follow(true);
            return;
        }
        if (event["method"] !== STATE || event["version"] !== 11) return;
        const change = record(params["change"]);
        const revision = change?.["revision"];
        if (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 0) {
            this.requestSnapshot();
            return;
        }
        if (this.current && revision <= this.current.revision) return;
        const state = record(change?.["conversationState"]);
        if (change?.["type"] === "snapshot" && state) {
            this.current = {revision, state};
        } else if (change?.["type"] === "patches") {
            try {
                if (!this.current || this.current.revision !== change["baseRevision"] || !Array.isArray(change["patches"])) {
                    throw new Error("Missing window snapshot or patch revision");
                }
                this.current = {revision, state: applyWindowPatches(this.current.state, change["patches"])};
            } catch {
                this.requestSnapshot();
                return;
            }
        } else {
            this.requestSnapshot();
            return;
        }
        this.publish();
    }

    private requestSnapshot(): void {
        if (!this.current) return;
        this.current = undefined;
        this.follow(true);
        this.publish();
    }

    private reset(): void {
        this.generation++;
        this.owner = undefined;
        this.current = undefined;
        this.transport = undefined;
        this.publish();
    }

    private publish(): void {
        this.changed(this.current);
        for (const listener of this.listeners) listener();
    }
}
