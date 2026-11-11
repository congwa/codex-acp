import {randomUUID} from "node:crypto";
import {lstat} from "node:fs/promises";
import {createConnection} from "node:net";
import {dirname, isAbsolute} from "node:path";

export type RecordValue = Record<string, unknown>;

export function record(value: unknown): RecordValue | undefined {
    return value !== null && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : undefined;
}

const MAX_FRAME_SIZE = 64 * 1024 * 1024;

async function* messages(socket: ReturnType<typeof createConnection>): AsyncGenerator<RecordValue> {
    const header = Buffer.alloc(4);
    let headerSize = 0;
    let payload: Buffer | undefined;
    let payloadSize = 0;
    for await (const chunk of socket as AsyncIterable<Buffer>) {
        let offset = 0;
        while (offset < chunk.length) {
            if (payload === undefined) {
                const copied = chunk.copy(header, headerSize, offset, offset + 4 - headerSize);
                headerSize += copied;
                offset += copied;
                if (headerSize < 4) continue;
                const length = header.readUInt32LE();
                if (length === 0 || length > MAX_FRAME_SIZE) throw new Error("Invalid Codex IPC frame size");
                payload = Buffer.allocUnsafe(length);
                headerSize = 0;
            }
            const copied = chunk.copy(payload, payloadSize, offset, offset + payload.length - payloadSize);
            payloadSize += copied;
            offset += copied;
            if (payloadSize !== payload.length) continue;
            const message = record(JSON.parse(payload.toString("utf8")));
            if (message === undefined) throw new Error("Invalid Codex IPC message");
            payload = undefined;
            payloadSize = 0;
            yield message;
        }
    }
}

export class WindowRequestError extends Error {
    constructor(readonly reason: string, readonly deliveryUnknown: boolean) {
        super(`Codex window IPC: ${reason}`);
    }
}

export function isWindowSocketUnavailable(error: unknown): boolean {
    const code = (error as NodeJS.ErrnoException | undefined)?.code;
    return code === "ENOENT" || code === "ECONNREFUSED";
}

interface Pending {
    resolve(message: RecordValue): void;
    reject(error: Error): void;
    timer: NodeJS.Timeout;
}

export interface WindowService {
    canHandle(request: RecordValue): boolean;
    handle(request: RecordValue): Promise<unknown>;
}

export class WindowTransport {
    private id = "initializing-client";
    private readonly pending = new Map<string, Pending>();
    private closed = false;

    private constructor(
        private readonly socket: ReturnType<typeof createConnection>,
        private readonly onEvent: (event: RecordValue) => void,
        private readonly onClose: (error: Error) => void,
        private readonly service?: WindowService,
    ) {
        void this.receive();
    }

    static async connect(
        path: string, onEvent: (event: RecordValue) => void, onClose: (error: Error) => void,
        service?: WindowService,
    ): Promise<WindowTransport> {
        const uid = process.getuid?.();
        if (!isAbsolute(path) || uid === undefined) throw new Error("An absolute Unix socket path is required");
        const directory = await lstat(dirname(path));
        const endpoint = await lstat(path);
        if (!directory.isDirectory() || !endpoint.isSocket() || directory.uid !== uid || endpoint.uid !== uid
            || (directory.mode & 0o077) !== 0 || (endpoint.mode & 0o077) !== 0) {
            throw new Error("The Codex window IPC socket and its directory must be private and owned by the current user");
        }
        const transport = new WindowTransport(createConnection(path), onEvent, onClose, service);
        try {
            const response = await transport.request("initialize", 0, {clientType: "agents-anywhere"}, undefined, 4000);
            const id = record(response["result"])?.["clientId"];
            if (typeof id !== "string") throw new Error("Codex window IPC did not assign a client ID");
            transport.id = id;
            return transport;
        } catch (error) {
            transport.close();
            throw error;
        }
    }

    request(method: string, version: number, params: RecordValue, owner?: string, timeoutMs = 6000): Promise<RecordValue> {
        if (this.closed) return Promise.reject(new WindowRequestError("disconnected", false));
        const requestId = randomUUID();
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(requestId);
                reject(new WindowRequestError("request-timeout", true));
            }, timeoutMs);
            this.pending.set(requestId, {resolve, reject, timer});
            try {
                this.send({type: "request", method, version, params, requestId, timeoutMs, ...(owner === undefined ? {} : {targetClientId: owner})});
            } catch (error) {
                this.pending.delete(requestId);
                clearTimeout(timer);
                reject(error);
            }
        });
    }

    broadcast(method: string, version: number, params: RecordValue, targets?: string[]): void {
        this.send({type: "broadcast", method, version, params, ...(targets ? {targetClientIds: targets} : {})});
    }

    close(): void {
        this.finish(new WindowRequestError("disconnected", true));
    }

    private send(message: RecordValue): void {
        const payload = Buffer.from(JSON.stringify({...message, sourceClientId: this.id}));
        if (payload.length > MAX_FRAME_SIZE) throw new Error("Codex window IPC frame exceeds the size limit");
        const frame = Buffer.allocUnsafe(4 + payload.length);
        frame.writeUInt32LE(payload.length);
        payload.copy(frame, 4);
        this.socket.write(frame);
    }

    private async receive(): Promise<void> {
        try {
            for await (const message of messages(this.socket)) {
                switch (message["type"]) {
                    case "response": {
                        const requestId = message["requestId"];
                        if (typeof requestId !== "string") break;
                        const pending = this.pending.get(requestId);
                        if (pending === undefined) break;
                        this.pending.delete(requestId);
                        clearTimeout(pending.timer);
                        if (message["resultType"] === "success") pending.resolve(message);
                        else {
                            const reason = typeof message["error"] === "string" ? message["error"] : "unknown-response";
                            pending.reject(new WindowRequestError(reason, !["no-client-found", "request-version-mismatch", "no-handler-for-request"].includes(reason)));
                        }
                        break;
                    }
                    case "client-discovery-request": {
                        const request = record(message["request"]);
                        this.send({type: "client-discovery-response", requestId: message["requestId"],
                            response: {canHandle: request !== undefined && this.service?.canHandle(request) === true}});
                        break;
                    }
                    case "request":
                        void this.serve(message);
                        break;
                    case "broadcast":
                        this.onEvent(message);
                        break;
                }
            }
            this.finish(new WindowRequestError("disconnected", true));
        } catch (error) {
            this.finish(error instanceof Error ? error : new Error(String(error)));
        }
    }

    private async serve(request: RecordValue): Promise<void> {
        const response = {type: "response", requestId: request["requestId"], method: request["method"], handledByClientId: this.id};
        try {
            if (!this.service?.canHandle(request)) throw new Error("no-handler-for-request");
            const result = await this.service.handle(request);
            if (!this.closed) this.send({...response, resultType: "success", result});
        } catch (error) {
            if (!this.closed) this.send({...response, resultType: "error", error: error instanceof Error ? error.message : "request-failed"});
        }
    }

    private finish(error: Error): void {
        if (this.closed) return;
        this.closed = true;
        this.socket.destroy();
        for (const pending of this.pending.values()) {
            clearTimeout(pending.timer);
            pending.reject(error);
        }
        this.pending.clear();
        this.onClose(error);
    }
}
